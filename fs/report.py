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
                Override writexml to remove XML declaration "<?xml version="1.0"?>"
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

    def table(self, rowdata):
        tableNode = self.doc.createElement("TABLE")

    # Shortcut functions
    def H(self, level, value=""):
        assert level in [1, 2, 3, 4, 5, 6]
        headingNode = self.doc.createElement("H%d" % level)
        textNode = self.doc.createTextNode(value)
        headingNode.appendChild(textNode)
        return headingNode
        
    def head(self, title="", meta=None):
        head = self.tag("head")
        head.appendChild(self.tag("title", value=title))
        return head

    def table(self, rowdata, attrs=None):
        tableNode = self.tag("table", attrs=attrs)
        for row in rowdata:
            rowNode = self.tag("tr")
            for v in row:
                rowNode.appendChild(self.tag("td", value=v))
            tableNode.appendChild(rowNode)
        return tableNode

    # Persistence
    def writehtml(self, writer):
        writer.write("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01??EN\" "
            "\"http://www.w3.org/TR/html4/strict.dtd\">\n")
        self.doc.writexml(writer, newl="\n")

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
        linkattrs = {}
        linkattrs["rel"] = "stylesheet"
        linkattrs["type"] = "text/css"
        linkattrs["href"] = "%s" % self.CSS_FILE
        head.appendChild(self.doc.tag("link", attrs=linkattrs))
        self.doc.add(head)

    def runtime_summary(self):
        html_contents = []
        html_contents.append(self.doc.H(self.SECTION_SIZE, "Runtime Summary"))

        runtime = self.get_runtime()
        rows = []
        rows.append(["ParaMark", "v%s, %s" % (runtime["version"],
            runtime["date"])])
        rows.append(["Platform", "%s" % runtime["platform"]])
        rows.append(["Target", "%s (%s)" % (runtime["wdir"], runtime["mountpoint"])])
        rows.append(["Time", "%s --- %s (%.5f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"])))])
        rows.append(["User","%s (%s)" % (runtime["user"], runtime["uid"])])
        rows.append(["Command line", "%s" % runtime["cmdline"]])
        rows.append(["Configuration", "fsbench.conf"])
        rows.append(["Results Data", "fsbench.db"])
        html_contents.append(self.doc.table(rows))

        return html_contents
    
    def _write_file(self):
        htmlFile = open("%s/%s" % (self.rdir, self.HTML_FILE), "w")
        self.doc.writehtml(htmlFile)
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
        for c in self.runtime_summary():
            body.appendChild(c)

        end = (time.localtime(), timer())
        self._write_file()

class HTMLReport2(Report):
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
        self.FILENAME = "report.html"
        self.TITLE = "ParaMark Filesytem Benchmark Report"
        self.TITLE_SIZE = 2
        self.SECTION_SIZE = self.TITLE_SIZE + 1
        self.SUBSECTION_SIZE = self.SECTION_SIZE + 1

        # Turn off html.verbose
        html.PRINTECHO = 0 
        
        self.doc = html.SimpleDocument(title=self.TITLE)

    # HTML sections
    def heading(self):
        self.doc.append(html.Heading(self.TITLE_SIZE, self.TITLE))
        
    def runtime_section(self):
        runtime = self.get_runtime()
        table = html.Table(tabletitle=None,
            heading=[], border=0, width="100%", cell_padding=2, cell_spacing=0,
            column1_align="left", cell_align="left")
        table.body = []
        table.body.append([html.Emphasis("ParaMark"), 
            html.Text(": v%s, %s" % (runtime["version"], runtime["date"]))])
        table.body.append([html.Emphasis("Platform"), 
            html.Text(": %s" % runtime["platform"])])
        table.body.append([html.Emphasis("Target"), 
            html.Text(": %s (%s)" % (runtime["wdir"], 
                runtime["mountpoint"]))])
        table.body.append([html.Emphasis("Time"), 
            html.Text(": %s --- %s (%.5f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"]))))])
        table.body.append([html.Emphasis("User"), 
            html.Text(": %s (%s)" % (runtime["user"], runtime["uid"]))])
        table.body.append([html.Emphasis("Command line"), 
            html.Text(": %s" % runtime["cmdline"])])
        datastr = html.Text(":")
        datastr.append(html.Href("../fsbench.conf", "fsbench.conf"))
        table.body.append([html.Emphasis("Configuration"), datastr])
        datastr = html.Text(":")
        datastr.append(html.Href("../fsbench.db", "fsbench.db"))
        table.body.append([html.Emphasis("Results Database"), datastr])

        self.doc.append(html.Heading(self.SECTION_SIZE, "Runtime Summary"))
        self.doc.append(table)

    def metadata_section(self):
        self.doc.append(html.Heading(self.SECTION_SIZE, 
            "Metadata Performance"))
        
        stats = self.meta_stats()
        for testid, tests_stat in stats:
            self.doc.append(html.Heading(self.SUBSECTION_SIZE, 
                "Performance of Test %d (#ops/sec)" % testid))
            table = html.Table(
                tabletitle="",
                heading=["Oper", "Avg", "Min", "Max", "StdDev", "Dist"],
                heading_align="left",
                border=0, width="100%", cell_padding=0, cell_spacing=2,
                column1_align="left", cell_align="left")
            table.body = []
            for oper, avg, mn, mx, stddev, fig in tests_stat:
                figbasename = os.path.basename(fig)
                img = html.Href("figures/%s" % figbasename,
                    html.Image(border=1, alight="center",
                    width=20, height=20, filename=figbasename,
                    src="figures/%s" % figbasename))
                table.body.append([oper, avg, mn, mx, stddev, img])
            self.doc.append(table)
    
    def io_section(self):
        self.doc.append(html.Heading(self.SECTION_SIZE, 
            "I/O Performance"))
        
        stats = self.io_stats()
        for testid, tests_stat in stats:
            self.doc.append(html.Heading(self.SUBSECTION_SIZE, 
                "Performance of Test %d" % testid))
            table = html.Table(
                tabletitle="",
                heading=["Oper", "Avg", "Min", "Max", "StdDev", "Dist"],
                heading_align="left",
                border=0, width="100%", cell_padding=0, cell_spacing=2,
                column1_align="left", cell_align="left")
            table.body = []
            for oper, avg, mn, mx, stddev, fig in tests_stat:
                figbasename = os.path.basename(fig)
                img = html.Href("figures/%s" % figbasename,
                    html.Image(border=1, alight="center",
                    width=20, height=20, filename=figbasename,
                    src="figures/%s" % figbasename))
                table.body.append([oper, avg, mn, mx, stddev, img])
            self.doc.append(table)

    def footnote(self, start, end):
        text = html.Small()
        text.append(html.Emphasis(
            "Generated at %s, took %.5f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", end[0]),
              (end[1] - start[1]))))
        
        text.append(html.Emphasis(html.Href("../report.conf", "report.conf")))
        text.append(" by ")
        text.append(html.Emphasis(html.Href(version.PARAMARK_WEB, "ParaMark")))
        text.append(html.Emphasis(" v%s, %s.\n" 
            % (version.PARAMARK_VERSION, version.PARAMARK_DATE)))
        
        self.doc.append(html.Paragraph(text))
    
    def produce(self):
        start = (time.localtime(), timer())
        
        self.heading()
        self.runtime_section()
        self.metadata_section()
        self.io_section()

        end = (time.localtime(), timer())

        self.footnote(start, end)
        
        # output
        self.doc.write("%s/%s" % (self.rdir, self.FILENAME))
        sys.stdout.write("Report generated to %s.\n" % self.rdir)

#########################################################################
# Auxiliary Utilities
#########################################################################
def html_fighref(filename, figsdir="figures"):
    """return an HTML href suffix string link to the target figure
    file"""
    basename = os.path.basename(filename)
    return html.Href("%s/%s" % (figsdir, basename),
        "%s" % basename.split(".")[-1].upper())

def html_tabhref(filename, text="HTML", tabsdir="tables"):
    """return an HTML href suffix string link to the target table 
    file"""
    basename = os.path.basename(filename)
    return html.Href("%s/%s" % (tabsdir, basename), text)

def html_sub(maintxt, subtxt):
    t = html.Text(maintxt)
    t.append(html.Sub(subtxt))
    return t

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
TITLE {
color: #000000;
display: block;
}

TITLE[class=main] {
font-size: 20pt;
}

TITLE[class=section] {
font-size: 16pt;
}
"""
