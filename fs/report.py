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
# fsreport.py
# File System Performance Report
#

import sys
import os
import time
import shutil
import ConfigParser
import StringIO
import xml.dom.minidom
import xml.sax.saxutils

import numpy
#import HTMLgen as html

import version 
import utils
from common import *
import bench
import data
import plot

class Report():
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = data.Database("%s/fsbench.db" % self.datadir, False)
        self.plot = plot.Plot()
        
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
                thputavg = numpy.average(thputlist)
                thputmin = numpy.min(thputlist)
                thputmax = numpy.max(thputlist)
                thputstddev = numpy.std(thputlist)
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

class HTMLDocument():
    def __init__(self):
        class DOMDocument(xml.dom.minidom.Document):
            def __init__(self):
                xml.dom.minidom.Document.__init__(self)

            def writexml(self, writer, indent="", addindent="", newl="",
                encoding=None):
                """
                Override writexml to remove XML declaration 
                "<?xml version="1.0"?>"
                """
                for node in self.childNodes:
                    node.writexml(writer, indent, addindent, newl)
    
        self.doc = DOMDocument()
        self.root = self.doc.createElement("HTML")
        self.doc.appendChild(self.root)

    def tag(self, name, value=None, attrs=None):
        name = name.upper()
        node = self.doc.createElement(name)
        if value:
            valueNode = self.doc.createTextNode("%s" % value)
            node.appendChild(valueNode)
        if attrs:
            for k, v in attrs.items():
                node.setAttribute(k, v)
        return node
    
    def add(self, node):
        self.root.appendChild(node)

    # Shortcut functions
    def H(self, level, value=""):
        assert level in [1, 2, 3, 4, 5, 6]
        headingNode = self.doc.createElement("H%d" % level)
        headingNode.appendChild(self.doc.createTextNode("%s" % value))
        return headingNode

    def text(self, txt=""):
        return self.doc.createTextNode("%s" % txt)
        
    def head(self, title="", meta=None):
        head = self.tag("head")
        head.appendChild(self.tag("title", value=title))
        return head

    def href(self, src, dest):
        aNode = self.doc.createElement("A")
        aNode.setAttribute("href", dest)
        if not isinstance(src, xml.dom.Node):
            src = self.doc.createTextNode("%s" % src)
        aNode.appendChild(src)
        return aNode

    def table(self, head, rowdata, attrs=None):
        tableNode = self.tag("table", attrs=attrs)
        trNode = self.tag("tr")
        for row in head + rowdata:
            if row in head:
                cellTag = "th"
            else:
                cellTag = "td"
            rowNode = self.tag("tr")
            for v in row:
                cellNode = self.tag(cellTag)
                if not isinstance(v, xml.dom.Node):
                    v = self.doc.createTextNode("%s" % v)
                cellNode.appendChild(v)
                rowNode.appendChild(cellNode)
            tableNode.appendChild(rowNode)
        return tableNode

    def img(self, src, attrs={}):
        attrs["src"] = src
        imgNode = self.tag("img", attrs=attrs)
        return imgNode

    # Persistence
    def writehtml(self, writer, newl=""):
        writer.write("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" "
            "\"http://www.w3.org/TR/html4/strict.dtd\">\n")
        self.doc.writexml(writer, newl=newl)

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
        self.HTML_FILE = "report.html"
        self.CSS_FILE = "report.css"
        self.TITLE = "ParaMark Filesytem Benchmarking Report"
        self.TITLE_SIZE = 1
        self.SECTION_SIZE = self.TITLE_SIZE + 1
        self.SUBSECTION_SIZE = self.SECTION_SIZE + 1

        self.doc = HTMLDocument()
        head = self.doc.head(title=self.TITLE)
        linkattrs = {"rel":"stylesheet", "type":"text/css", 
            "href":"%s" % self.CSS_FILE}
        head.appendChild(self.doc.tag("link", attrs=linkattrs))
        self.doc.add(head)

    def runtime_summary(self):
        html_contents = []
        html_contents.append(self.doc.H(self.SECTION_SIZE, "Runtime Summary"))

        runtime = self.get_runtime()
        rows = []
        rows.append(
            ["ParaMark", "v%s, %s" % (runtime["version"], runtime["date"])])
        rows.append(["Platform", "%s" % runtime["platform"]])
        rows.append(
            ["Target", "%s (%s)" % (runtime["wdir"], runtime["mountpoint"])])
        rows.append(
            ["Time", "%s --- %s (%.5f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"])))])
        rows.append(["User","%s (%s)" % (runtime["user"], runtime["uid"])])
        rows.append(["Command", "%s" % runtime["cmdline"]])
        hrefNode = self.doc.href("fsbench.conf", "../fsbench.conf")
        rows.append(["Configuration", hrefNode])
        hrefNode = self.doc.href("fsbench.db", "../fsbench.db")
        rows.append(["Data", hrefNode])
        html_contents.append(self.doc.table([], rows))

        return html_contents

    def metadata_section(self):
        html_contents = []
        html_contents.append(self.doc.H(self.SECTION_SIZE, 
            "Metadata Performance"))
        
        for testid, tests_stat in self.meta_stats():
            rows = []
            head = [["op", "avg", "min", "max", "stddev", "dist"]]
            for oper, avg, mn, mx, stddev, fig in tests_stat:
                figpath = "figures/%s" % os.path.basename(fig)
                fighref = self.doc.href(self.doc.img(figpath), figpath)
                rows.append([oper, avg, mn, mx, stddev, fighref])
            html_contents.append(self.doc.table(head, rows,
                attrs={"class":"data"}))
        return html_contents
    
    def io_section(self):
        html_contents = []
        html_contents.append(self.doc.H(self.SECTION_SIZE, "I/O Performance"))
        return html_contents

    def footnote(self, start, end):
        html_contents = []
        pNode = self.doc.tag("p", 
            value="Generated at %s, took %.5f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", end[0]),
              (end[1] - start[1])),
            attrs={"class":"footnote"})
        pNode.appendChild(self.doc.href("report.conf", "../report.conf"))
        pNode.appendChild(self.doc.text(" by "))
        pNode.appendChild(self.doc.href("ParaMark", version.PARAMARK_WEB))
        pNode.appendChild(self.doc.text(" v%s, %s.\n" 
            % (version.PARAMARK_VERSION, version.PARAMARK_DATE)))

        html_contents.append(pNode)
        return html_contents
    
    def _write_file(self):
        htmlFile = open("%s/%s" % (self.rdir, self.HTML_FILE), "w")
        self.doc.writehtml(htmlFile, newl="\n")
        htmlFile.close()
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARAMARK_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
        sys.stdout.write("Report generated to %s.\n" % self.rdir)
        
    def write(self):
        start = (time.localtime(), timer())

        body = self.doc.tag("body")
        self.doc.add(body)

        body.appendChild(self.doc.H(self.TITLE_SIZE, value=self.TITLE))
        body_contents = []
        body_contents.extend(self.runtime_summary())
        body_contents.extend(self.metadata_section())
        body_contents.extend(self.io_section())

        for c in body_contents:
            body.appendChild(c)
        
        end = (time.localtime(), timer())
        for c in self.footnote(start, end):
            body.appendChild(c)

        self._write_file()

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

IMG {
border-style: outset;
border-width: 1px;
width: 20px;
height: 20px;
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
padding: 6px 8px;
}
"""
