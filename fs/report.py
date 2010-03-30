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
import modules.utils as utils
import modules.plot as plot
import modules.DHTML as DHTML
import bench
import data

from bench import OPTYPE_META, OPTYPE_IO, FSOP_META, FSOP_IO

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

    def meta_stats(self):
        res = {}
        for oper in self.db.meta_get_opers():
            res[oper] = {}
            for host in self.db.meta_get_hosts():
                res[oper][host] = self.db.meta_stats_by_host(oper, host)
        return res
    
    def io_stats(self):
        res = {}
        for oper in self.db.io_get_opers():
            res[oper] = {}
            for host in self.db.io_get_hosts():
                res[oper][host] = self.db.io_stats_by_host(oper, host)
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
        self.SIDEBAR_SIZE = 20
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
        
        # runtime summary
        body.appendChild(doc.H(self.SECTION_SIZE, "Runtime Summary"))
        runtime = {}
        for k, v in self.db.get_runtimes():
            runtime[k] = v
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
        meta_opers = []
        meta_aggs = []
        meta_stds = []
        io_opers = []
        io_aggs = []
        io_stds = []
        for tid,op,optype,thmin,thmax,thavg,thagg,thstd \
            in self.db.get_stat_all():
            if optype == OPTYPE_META:
                meta_opers.append(op)
                meta_aggs.append(thagg)
                meta_stds.append(thstd)
            elif optype == OPTYPE_IO:
                io_opers.append(op)
                io_aggs.append(thagg)
                io_stds.append(thstd)
            
        if len(meta_aggs) > 0:
            body.appendChild(doc.H(self.SECTION_SIZE, "Metadata Performance"))
            figpath = "%s/meta_summary.png" % self.fdir
            figlink = "figures/meta_summary.png"
            self.pyplot.bar(figpath, meta_aggs, yerr=meta_stds, 
                xticks=meta_opers,
                title="Overall Metadata Performance",
                xlabel="Metadata Operations",
                ylabel="Performance (ops/sec)")
            body.appendChild(doc.HREF(doc.IMG(figlink, 
               attrs={"class":"demo"}), figlink))
        
        if len(io_aggs) > 0:
            body.appendChild(doc.H(self.SECTION_SIZE, "I/O Performance"))
            figpath = "%s/io_summary.png" % self.fdir
            figlink = "figures/io_summary.png"
            # TODO: Setup unit from options
            io_aggs = map(lambda x:x/1048576, io_aggs)
            io_stds = map(lambda x:x/1048576, io_stds)
            self.pyplot.bar(figpath, io_aggs, yerr=io_stds, 
                xticks=io_opers,
                title="Overall I/O Performance",
                xlabel="I/O Operations",
                ylabel="Performance (MB/sec)")
            body.appendChild(doc.HREF(doc.IMG(figlink, 
               attrs={"class":"demo"}), figlink))

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

    def navi_page(self, metapages, iopages):
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
            subitems.append((op, {"onClick":"showPage(this)", "ref":page}, []))
        items.append(("Metadata", {}, subitems))
        
        subitems = []
        for op, page in iopages:
            subitems.append((op, {"onClick":"showPage(this)", "ref":page}, []))
        items.append(("Input/Output", {}, subitems))
        
        body.appendChild(doc.makeList(items, attrs={"class":"navi"}))
        htmlFile = open("%s/%s" % (self.rdir, self.NAVI_FILE), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

    def oper_page(self, opname):
        doc = DHTML.HTMLDocument()
        head = doc.makeHead(opname)
        head.appendChild(doc.tag("link", attrs=self.LINK_ATTRS))
        doc.add(head)
        
        body = doc.tag("body")
        doc.add(body)

        body.appendChild(doc.H(self.TITLE_SIZE, value="%s" % opname))
        body.appendChild(doc.H(self.SECTION_SIZE, "Comparison among hosts"))
        
        tHead = [["host", "agg", "avg", "min", "max", "std", "dist"]]
        tRows = []
        avgList = []
        aggList = []
        stdList = []
        hostList = []
        for hid,tid,optype,thmin,thmax,thavg,thagg,thstd \
            in self.db.get_stats('data1',
            ['hostid','tid','optype','min','max','avg','agg','std'], 
            {'oper':opname}):
            
            figpath = "%s/%s_host%s.png" % (self.fdir, opname, hid)
            figlink = "figures/%s_host%s.png" % (opname, hid)
            thlist = self.db.get_stats('data0', ['agg'], 
                {'hostid':hid, 'oper':opname})
            if optype == OPTYPE_IO:
                thlist = map(lambda x:x/1048576, thlist)
                thagg = thagg / 1048576
                thavg = thavg / 1048576
                thmin = thmin / 1048576
                thmax = thmax / 1048576
                thstd = thstd / 1048576
            self.pyplot.point(figpath, thlist)
            figref = doc.HREF(doc.IMG(figlink, attrs={"class":"thumbnail"}), 
                    figlink)
            tRows.append([hid, thagg, thavg, thmin, thmax, thstd, figref])
            avgList.append(thavg)
            aggList.append(thagg)
            stdList.append(thstd)
            hostList.append(hid)

        figpath = "%s/%s_host_cmp.png" % (self.fdir, opname)
        figlink = "figures/%s_host_cmp.png" % opname
        self.pyplot.bar(figpath, aggList, yerr=stdList, xticks=hostList)
        body.appendChild(doc.HREF(doc.IMG(figlink, attrs={"class":"demo"}),
            figlink))
        body.appendChild(doc.table(tHead, tRows, attrs={"class":"data"}))

        htmlFile = open("%s/%s.html" % (self.rdir, opname), "w")
        doc.write(htmlFile, newl="\n")
        htmlFile.close()

        return "%s.html" % opname

    def css_file(self):
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARAMARK_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
        sys.stdout.write("Report generated to %s.\n" % self.rdir)
        
    def write(self):
        self.start = utils.timer2()
        
        self.db.agg_thread(True)
        self.db.agg_host(True)
        self.db.agg_all(True)

        metapages = []
        iopages = []
        for oper in self.db.get_opers():
            if oper in FSOP_META:
                metapages.append((oper, self.oper_page(oper)))
            elif oper in FSOP_IO:
                iopages.append((oper, self.oper_page(oper)))
        self.index_page()
        self.navi_page(metapages, iopages)
        self.css_file()
        self.main_page()

##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

PARAMARK_DEFAULT_REPORT_CONFIG_STRING = """\
# ParaMark default performance report configuration
# 2010/03/15

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
