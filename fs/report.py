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
        res = {}
        for oper in self.db.meta_get_opers():
            res[oper] = {}
            for host in self.db.meta_get_hosts():
                res[oper][host] = self.db.meta_stats_by_host(oper, host)
        return res
    
    def io_stats(self):
        res = {}
        return res

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
    
    def main_page(self, meta_stats, io_stats):
        doc = DHTML.HTMLDocument()
        head = doc.makeHead(title=self.TITLE)
        head.appendChild(doc.tag("link", attrs=self.LINK_ATTRS))
        doc.add(head)
        
        body = doc.tag("body")
        doc.add(body)
        
        body.appendChild(doc.H(self.TITLE_SIZE, value=self.TITLE))
        
        # runtime summary
        body.appendChild(doc.H(self.SECTION_SIZE, "Runtime Summary"))
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
        rows.append(["Configuration",
            doc.HREF("fsbench.conf", "../fsbench.conf")])
        rows.append(["Data", 
            doc.HREF("fsbench.db", "../fsbench.db")])
        body.appendChild(doc.table([], rows))
        
        # metadata summary
        opers = []
        avgs = []
        stds = []
        for oper, hostdat in meta_stats.items():
            values = map(lambda x:x[0], hostdat.values())
            avgs.append(numpy.average(values))
            stds.append(numpy.std(values))
            opers.append(oper)
        if len(opers) > 0:
            body.appendChild(doc.H(self.SECTION_SIZE, "Metadata Performance"))
            figpath = "%s/meta_summary.png" % self.fdir
            figlink = "figures/meta_summary.png"
            self.pyplot.bar(figpath, avgs, yerr=stds, xticks=opers,
                title="Overall Metadata Performance",
                xlabel="Metadata Operations",
                ylabel="Performance (ops/sec)")
            body.appendChild(doc.HREF(doc.IMG(figlink, 
                attrs={"class":"demo"}), figlink))

        # io summary
        body.appendChild(doc.H(self.SECTION_SIZE, "I/O Performance"))
        oper = []
        avgs = []
        stds = []
        for oper, hostdat in io_stats.items():
            print oper, hostdat

        # footnote
        self.end = utils.timer2()
        pNode = doc.tag("p", 
            value="Generated at %s, took %.2f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", self.end[0]),
              (self.end[1] - self.start[1])),
            attrs={"class":"footnote"})
        pNode.appendChild(doc.HREF("report.conf", "../report.conf"))
        pNode.appendChild(doc.TEXT(" by "))
        pNode.appendChild(doc.HREF("ParaMark", version.PARAMARK_WEB))
        pNode.appendChild(doc.TEXT(" v%s, %s." 
            % (version.PARAMARK_VERSION, version.PARAMARK_DATE)))
        body.appendChild(pNode)
        
        htmlFile = open("%s/%s" % (self.rdir, self.MAIN_FILE), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

    def navi_page(self, metapages):
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
        for op, page in metapages:
            subitems.append((op, {"onClick":"showPage(this)",
                 "ref":page}, []))
        items.append(("Metadata", {}, subitems))
        
        subitems = []
        for op in self.db.io_get_opers():
            subitems.append((op, {}, []))
        items.append(("Input/Output", {}, []))
        
        body.appendChild(doc.makeList(items, attrs={"class":"navi"}))
        htmlFile = open("%s/%s" % (self.rdir, self.NAVI_FILE), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

    def meta_pages(self):
        metapages = []
        metastats = self.meta_stats()
        for oper, hostdat in metastats.items():
            metapages.append((oper, self.meta_oper_page(oper, hostdat)))
        return metapages, metastats


    def meta_oper_page(self, opname, hostdat):
        doc = DHTML.HTMLDocument()
        head = doc.makeHead(opname)
        head.appendChild(doc.tag("link", attrs=self.LINK_ATTRS))
        body = doc.tag("body")
        doc.add(head)
        
        body = doc.tag("body")
        doc.add(body)
        body.appendChild(doc.H(self.TITLE_SIZE, value="metadata:%s" % opname))

        body.appendChild(doc.H(self.SECTION_SIZE, "Performance among hosts"))
        
        tHead = [["host", "avg", "min", "max", "std", "dist"]]
        tRows = []
        avgList = []
        stdList = []
        hostList = []
        for host, dat in hostdat.items():
            thputavg, thputmin, thputmax, thputstd, thputlist = dat            
            figpath = "%s/%s_host%s.png" % (self.fdir, opname, host)
            figlink = "figures/%s_host%s.png" % (opname, host) 
            self.pyplot.point(figpath, thputlist)
            figref = doc.HREF(doc.IMG(figlink, attrs={"class":"thumbnail"}), 
                    figlink)
            tRows.append([host, thputavg, thputmin, thputmax, thputstd, 
                figref])
            avgList.append(thputavg)
            stdList.append(thputstd)
            hostList.append(host)

        figpath = "%s/%s_host_cmp.png" % (self.fdir, opname)
        figlink = "figures/%s_host_cmp.png" % opname
        self.pyplot.bar(figpath, avgList, yerr=stdList, xticks=hostList)
        body.appendChild(doc.HREF(doc.IMG(figlink, attrs={"class":"demo"}),
            figlink))
        body.appendChild(doc.table(tHead, tRows, attrs={"class":"data"}))

        htmlFile = open("%s/%s.html" % (self.rdir, opname), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

        return "%s.html" % opname

    def io_pages(self):
        iopages = []
        iostats = self.io_stats()
        for oper, hostdat in iostats.items():
            iopages.append((oper, self.io_oper_page(oper, hostdat)))
        return iopages, iostats
    
    def css_file(self):
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARAMARK_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
        sys.stdout.write("Report generated to %s.\n" % self.rdir)
        
    def write(self):
        self.start = utils.timer2()

        meta_pages, meta_stats = self.meta_pages()
        io_pages, io_stats = self.io_pages()
        self.index_page()
        self.navi_page(meta_pages)
        self.css_file()
        self.main_page(meta_stats, io_stats)


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
