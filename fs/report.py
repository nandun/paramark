#############################################################################
# ParaMark: A Parallel/Distributed File Systems Benchmark
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

#
# fs/report.py
# File System Performance Report
#

import sys
import os
import shutil
import time
import ConfigParser
import StringIO
import xml.dom.minidom

import numpy

import version 
import common.utils as utils
import common.plot as plot
import common.DHTML as DHTML
import bench
import data

class Report():
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = data.Database("%s/fsbench.db" % self.datadir, False)
        self.plot = plot.Plot()
        self.pyplot = plot.Pyplot()
        
        # report root dir
        self.rdir = os.path.abspath("%s/report" % self.datadir)
        if not os.path.exists(self.rdir):
            utils.smart_makedirs(self.rdir)
        # figures dir
        self.fdir = os.path.abspath("%s/figures" % self.rdir)
        if not os.path.exists(self.fdir):
            utils.smart_makedirs(self.fdir)
        # data dir
        self.ddir = os.path.abspath("%s/data" % self.rdir)
        if not os.path.exists(self.ddir):
            utils.smart_makedirs(self.ddir)
    
    def __del__(self):
        self.db.close()

    def get_runtime(self):
        runtime = {}
        for i, v in self.db.runtime_sel():
            runtime[i] = v
        return runtime

    def meta_stats(self):
        stats = []
        for testid in self.db.meta_get_testids():
            tests_stat = []
            for oper in self.db.meta_get_opers(testid=testid):
                thputlist = []
                tids = []
                for tid, data in self.db.meta_get_tid_and_data(
                    testid=testid, oper=oper):
                    tids.append(tid)
                    thputlist.append(len(data)/numpy.sum(map(lambda (s,e):e-s,
                        data)))
                thputavg = round(numpy.average(thputlist), 2)
                thputmin = round(numpy.min(thputlist), 2)
                thputmax = round(numpy.max(thputlist), 2)
                thputstddev = round(numpy.std(thputlist), 2)
                fig = self.plot.points_chart(
                    tids, thputlist,
                    title="Throughput Distribution of %s in Test %s"
                        % (oper, testid),
                    xlabel="Process ID", ylabel="Throughput (ops/sec)",
                    prefix="%s/dist-%s-%s" % (self.fdir, oper, testid))
                tests_stat.append((oper, thputavg, thputmin, thputmax,
                    thputstddev, fig))
            stats.append((testid, tests_stat))

        return stats

    def io_stats(self):
        stats = []
        for testid in self.db.io_get_testids():
            tests_stat = []
            for oper in self.db.io_get_opers(testid=testid):
                thputlist = []
                for data in self.db.io_get_data(testid=testid, oper=oper):
                    thputlist.append(
                        10*1048576/numpy.sum(map(lambda (s,e):e-s, data)))
                thputavg = numpy.average(thputlist)
                thputmin = numpy.min(thputlist)
                thputmax = numpy.max(thputlist)
                thputstddev = numpy.std(thputlist)
                fig = self.plot.points_chart(
                    range(0, len(thputlist)), thputlist,
                    title="Throughput Distribution of %s in Test %s"
                        % (oper, testid),
                    xlabel="Process", ylabel="Throughput (msec/sec)",
                    prefix="%s/dist-%s-%s" % (self.fdir, oper, testid))
                tests_stat.append((oper, thputavg, thputmin, thputmax,
                    thputstddev, fig))
            stats.append((testid, tests_stat))
        
        return stats

class HTMLReport(Report):
    def __init__(self, datadir):
        Report.__init__(self, datadir)
        
        # Load configurations from default to user specified
        cfg = ConfigParser.ConfigParser()
        defaultcfg = StringIO.StringIO(PARAMARK_DEFAULT_REPORT_CONFIG_STRING)
        cfg.readfp(defaultcfg)
        defaultcfg.close()

        if not os.path.exists("%s/report.conf" % self.datadir):
            fp = open("%s/report.conf" % self.datadir, "wb")
            fp.write(PARAMARK_DEFAULT_REPORT_CONFIG_STRING)
            fp.close()
        else:
            cfg.read("%s/report.conf")
        
        # Convert configs to options for convenience
        self.opts ={}
        section = "report"
        if cfg.has_section(section):
            for k, v in cfg.items(section):
                self.opts[k] = eval(v)
        
        self.opts["html"] = {}
        section = "html"
        if cfg.has_section(section):
            for k, v in cfg.items(section):
                self.opts[section][k] = eval(v)

        # HTML default settings
        self.INDEX_FILE = "index.html"
        self.NAVI_FILE = "navi.html"
        self.MAIN_FILE = "main.html"
        self.CSS_FILE = "style.css"
        self.TITLE = "ParaMark Filesytem Benchmarking Report"
        self.TITLE_SIZE = 1
        self.SECTION_SIZE = self.TITLE_SIZE + 1
        self.SUBSECTION_SIZE = self.SECTION_SIZE + 1
        self.SIDEBAR_SIZE = 10
        self.LINK_ATTRS = {"rel":"stylesheet", "type":"text/css", 
            "href":"%s" % self.CSS_FILE}

    def index_page(self):
        doc = DHTML.HTMLDocument()
        cols = "%s%%,%s%%" % (self.SIDEBAR_SIZE, 100 - self.SIDEBAR_SIZE)
        frameset = doc.tag("frameset", attrs={"cols":cols})
        frameset.appendChild(doc.tag("frame", 
            attrs={"name":"navi", "src":self.NAVI_FILE}))
        frameset.appendChild(doc.tag("frame", 
            attrs={"name":"content", "src":self.MAIN_FILE}))
        doc.add(frameset)
        
        htmlFile = open("%s/%s" % (self.rdir, self.INDEX_FILE), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()
    
    def main_page(self):
        doc = DHTML.HTMLDocument()
        head = doc.makeHead(title=self.TITLE)
        head.appendChild(doc.tag("link", attrs=self.LINK_ATTRS))
        doc.add(head)
        
        body = doc.tag("body")
        doc.add(body)

        body.appendChild(doc.H(self.TITLE_SIZE, value=self.TITLE))
        body_contents = []
        body_contents.extend(self.main_runtime_summary(doc))
        body_contents.extend(self.main_metadata_section(doc))
        body_contents.extend(self.main_io_section(doc))

        for c in body_contents:
            body.appendChild(c)
        
        self.end = utils.timer2()
        for c in self.footnote(doc, self.start, self.end):
            body.appendChild(c)
        
        htmlFile = open("%s/%s" % (self.rdir, self.MAIN_FILE), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

    def navi_page(self):
        doc = DHTML.HTMLDocument()
        head = doc.tag("head")
        body = doc.tag("body")
        doc.add(head)

        # insert script
        scriptTxt = \
"""
function showPage(item) {
    top.content.location = item.getAttribute('ref')
}
"""
        script = doc.tag("script", value=scriptTxt,
            attrs={"language":"javascript"})
        head.appendChild(script)
        link = doc.tag("link", attrs={"href":self.CSS_FILE, 
            "rel":"stylesheet", "type":"text/css"})
        head.appendChild(link)
        doc.add(body)

        items = [("Overview", 
            {"onClick":"showPage(this)","ref":self.MAIN_FILE}, [])]
        
        subitems = []
        for op in self.db.meta_get_opers():
            subitems.append((op, 
                {"onClick":"showPage(this)",
                 "ref":self.meta_page(op)}, 
                 []))
        items.append(("Metadata", {}, subitems))
        
        subitems = []
        for op in self.db.io_get_opers():
            subitems.append((op, {}, []))
        items.append(("Input/Output", {}, []))
        
        body.appendChild(doc.makeList(items, attrs={"class":"navi"}))
        htmlFile = open("%s/%s" % (self.rdir, self.NAVI_FILE), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

    def meta_page(self, opname):
        doc = DHTML.HTMLDocument()
        head = doc.makeHead(opname)
        head.appendChild(doc.tag("link", attrs=self.LINK_ATTRS))
        body = doc.tag("body")
        doc.add(head)
        
        body = doc.tag("body")
        doc.add(body)
        body.appendChild(doc.H(self.TITLE_SIZE, value="metadata:%s" % opname))
        
        htmlFile = open("%s/%s.html" % (self.rdir, opname), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

        return "%s.html" % opname

    def main_runtime_summary(self, doc):
        html_contents = []
        html_contents.append(doc.H(self.SECTION_SIZE, "Runtime Summary"))

        runtime = self.get_runtime()
        rows = []
        rows.append(
            ["ParaMark", "v%s, %s" % (runtime["version"], runtime["date"])])
        rows.append(["Platform", "%s" % runtime["platform"]])
        rows.append(
            ["Target", "%s (%s)" % (runtime["wdir"], runtime["mountpoint"])])
        rows.append(
            ["Time", "%s --- %s (%.2f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"])))])
        rows.append(["User","%s (%s)" % (runtime["user"], runtime["uid"])])
        rows.append(["Command", "%s" % runtime["cmdline"]])
        hrefNode = doc.HREF("fsbench.conf", "../fsbench.conf")
        rows.append(["Configuration", hrefNode])
        hrefNode = doc.HREF("fsbench.db", "../fsbench.db")
        rows.append(["Data", hrefNode])
        html_contents.append(doc.table([], rows))

        return html_contents

    def main_metadata_section(self, doc):
        html_contents = []
        html_contents.append(doc.H(self.SECTION_SIZE, 
            "Metadata Performance"))
        
        opers = []
        avgs = []
        stds = []
        for testid, tests_stat in self.meta_stats():
            rows = []
            head = [["op", "avg", "min", "max", "stddev", "dist"]]
            for oper, avg, mn, mx, stddev, fig in tests_stat:
                opers.append(oper)
                avgs.append(avg)
                stds.append(stddev)
                figpath = "figures/%s" % os.path.basename(fig)
                fighref = doc.HREF(
                    doc.IMG(figpath, attrs={"class":"thumbnail"}), 
                    figpath)
                rows.append([oper, avg, mn, mx, stddev, fighref])
            html_contents.append(doc.table(head, rows,
                attrs={"class":"data"}))
        
        figpath = "%s/meta_compare.png" % self.fdir
        self.pyplot.bar(figpath, avgs, yerr=stds, xticks=opers)
        fig = doc.HREF(doc.IMG(figpath, attrs={"class":"demo"}),
            figpath)
        html_contents.append(fig)

        return html_contents
    
    def main_io_section(self, doc):
        html_contents = []
        html_contents.append(doc.H(self.SECTION_SIZE, "I/O Performance"))
        return html_contents

    def footnote(self, doc, start, end):
        html_contents = []
        pNode = doc.tag("p", 
            value="Generated at %s, took %.2f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", end[0]),
              (end[1] - start[1])),
            attrs={"class":"footnote"})
        pNode.appendChild(doc.HREF("report.conf", "../report.conf"))
        pNode.appendChild(doc.TEXT(" by "))
        pNode.appendChild(doc.HREF("ParaMark", version.PARAMARK_WEB))
        pNode.appendChild(doc.TEXT(" v%s, %s." 
            % (version.PARAMARK_VERSION, version.PARAMARK_DATE)))

        html_contents.append(pNode)
        return html_contents
    
    def css_file(self):
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARAMARK_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
        sys.stdout.write("Report generated to %s.\n" % self.rdir)
        
    def write(self):
        self.start = utils.timer2()
        self.index_page()
        self.navi_page()
        self.css_file()
        self.main_page()


##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

PARAMARK_DEFAULT_REPORT_CONFIG_STRING = """\
# ParaMark default performance report configuration
# 2009/12/22

[report]
# report format: html
format = 'html'

[html]
# plot tools, 'gchar', 'gnuplot', or 'matplotlib'
plot = 'gnuplot'
imageformat = 'png'
"""

# Be careful about browser compatibility!
# CSS selector:
#   IE: NODE.class
#   Firefox: NODE[class=value]
PARAMARK_DEFAULT_CSS_STYLE_STRING = """\
H1 {
font-family: Arial;
}

H2 {
font-family: Arial;
background-color: #41a317;
}

P[class=footnote] {
font-family: Times New Roman;
font-size: 12pt;
font-style: italic;
display: block;
background-color: #99c68e;
}

IMG[class=thumbnail] {
border-style: outset;
border-width: 1px;
width: 20px;
height: 20px;
}

IMG[class=demo] {
display: block;
margin-left: auto;
margin-right: auto;
width: 800px;
height: 600px;
vertical-align: middle;
border-style: outset;
border-width: 0px;
}

TABLE {
font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;
font-size: 16px;
border-collapse: collapse;
text-align: left;
width: 100%;
}

TH {
font-size: 14px;
font-weight: bold;
padding: 6px 8px;
border-bottom: 2px solid;
}

TD {
padding: 2px 2px;
}

UL[class=navi] {
font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;
font-size: 14px;
cursor: pointer;
}
"""
