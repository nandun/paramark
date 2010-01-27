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
import numpy
import HTMLgen as html

import common
import fsop
import fsdata
import fsplot

class HTMLReport():
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = fsdata.Database("%s/fsbench.db" % self.datadir, False)
        self.plot = fsplot.Plot()
        
        # report root dir
        self.rdir = os.path.abspath("%s/report" % self.datadir)
        if not os.path.exists(self.rdir):
            common.smart_makedirs(self.rdir)
        # figures dir
        self.fdir = os.path.abspath("%s/figures" % self.rdir)
        if not os.path.exists(self.fdir):
            common.smart_makedirs(self.fdir)
        # data dir
        self.ddir = os.path.abspath("%s/data" % self.rdir)
        if not os.path.exists(self.ddir):
            common.smart_makedirs(self.ddir)
        
        # Load configurations from default to user specified
        cfg = ConfigParser.ConfigParser()
        defaultcfg = StringIO.StringIO(PARAMARK_DEFAULT_REPORT_CONFIG_STRING)
        cfg.readfp(defaultcfg)
        defaultcfg.close()

        if not os.path.exists("%s/report.conf" % self.datadir):
            fp = open("%s/report.conf" % self.datadir, "wb")
            fp.write(PARAMARK_DEFAULT_REPORT_CONFIG_STRING)
            fp.close()
        else: cfg.read("%s/report.conf")
        
        # Convert configs to options for convenience
        self.opts ={}
        section = "report"
        if cfg.has_section(section):
            for k, v in cfg.items(section): self.opts[k] = eval(v)
        
        self.opts["html"] = {}
        section = "html"
        if cfg.has_section(section):
            for k, v in cfg.items(section): self.opts[section][k] = eval(v)

        
        # HTML default settings
        self.FILENAME = "report.html"
        self.TITLE = "ParaMark Filesytem Benchmark Report"
        self.TITLE_SIZE = 2
        self.SECTION_SIZE = self.TITLE_SIZE + 1
        self.SUBSECTION_SIZE = self.SECTION_SIZE + 1

        # Turn off html.verbose
        html.PRINTECHO = 0 
        
        self.doc = html.SimpleDocument(title=self.TITLE)

    def __del__(self):
        self.db.close()

    # HTML sections
    def heading(self):
        self.doc.append(html.Heading(self.TITLE_SIZE, self.TITLE))
        
    def runtime_summary(self):
        runtime = {}
        for i, v in self.db.runtime_sel(): runtime[i] = v
        table = html.Table(tabletitle=None,
            heading=[], border=0, width="100%", 
            cell_padding=2, cell_spacing=0,
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

    def metadata(self):
        table = html.Table(tabletitle=None,
            heading=["Oper", "Test", "#ops", "Elapsed", 
                     "", "", "", "Throughput"],
            heading_align="right",
            border=0, width="100%", cell_padding=0, cell_spacing=2,
            column1_align="right", cell_align="right")

        table.body = []
        table.body.append(map(lambda s:html.Emphasis(s),
            ["", "", "", 
             "Total", "Avg.", "StdDev", "Dist.",
             "#ops/sec"]))
        
        for oper, tid, rdata, nops, esum, eavg, estddev, thput in \
            self.db.metadata_stats():
            opts = "%s" % tid
            
            # plot data
            # ATTENTION: choose end stamp as x-coordinates
            xdata = map(lambda (s,e):e, rdata)
            ydata = data
            self.plot.points_chart(xdata, ydata,
                title="%s performance with %s" 
                    % (oper, opts),
                prefix="%s/%s-%s" % (self.fdir, oper, opts))
            
            image = html.Href("figures/%s-%s.png" % (oper, opts), 
                html.Image(border=1, align="center",
                width=20,height=20,
                filename="%s-%s.png" % (oper, opts),
                src="figures/%s-%s.png" % (oper, opts)))

            table.body.append([oper, opts, nops, summ, avg, stddev, image,
            thput])

        self.doc.append(html.Heading(self.SECTION_SIZE, "Runtime Summary"))
        self.doc.append(table)

    def produce(self):
        start = (time.localtime(), common.timer())
        
        self.heading()
        self.runtime_summary()
        self.metadata()

        end = (time.localtime(), common.timer())
        
        # output
        self.doc.write("%s/%s" % (self.rdir, self.FILENAME))
        sys.stdout.write("Report generated to %s.\n" % self.rdir)
    
    def html_footnote(self, start, end):
        text = html.Small()
        text.append(html.Emphasis(
            "Generated at %s, took %.5f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", end[0]),
              (end[1] - start[1]))))
        
        text.append(html.Emphasis(html.Href("../report.conf", 
            "report.conf")))
        text.append(" by ")
        from version import PARAMARK_VERSION, PARAMARK_DATE, PARAMARK_WEB
        text.append(html.Emphasis(html.Href(PARAMARK_WEB, "ParaMark")))
        text.append(html.Emphasis(" v%s, %s.\n" 
            % (PARAMARK_VERSION, PARAMARK_DATE)))
        para = html.Paragraph(text)
        return para

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
