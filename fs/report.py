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

# fs/report.py
# Performance Report Generation

import sys
import os
import shutil
import time
import ConfigParser
import StringIO
import xml.dom.minidom

import version 
from modules.verbose import *
from modules.common import *
import modules.DHTML as DHTML
import modules.num as num
import bench
import data

from oper import TYPE_META, TYPE_IO, OPS_META, OPS_IO

LOGSCALE_THRESHOLD = 1000

REPORT_VERBOSE_LEVEL = 2

class Report():
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = data.Database("%s/fsbench.db" % self.datadir)
        
        # report root dir
        self.rdir = os.path.abspath("%s/report" % self.datadir)
        if not os.path.exists(self.rdir):
            smart_makedirs(self.rdir)
        # figures dir
        self.fdir = os.path.abspath("%s/figures" % self.rdir)
        if not os.path.exists(self.fdir):
            smart_makedirs(self.fdir)
        # data dir
        self.ddir = os.path.abspath("%s/data" % self.rdir)
        if not os.path.exists(self.ddir):
            smart_makedirs(self.ddir)
    
    def __del__(self):
        self.db.close()

    def runtime_stats(self):
        runtime = {}
        for k, v in self.db.get_runtimes():
            runtime[k] = v
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
        for oper in self.db.io_get_opers():
            res[oper] = {}
            for host in self.db.io_get_hosts():
                res[oper][host] = self.db.io_stats_by_host(oper, host)
        return res

class TextReport(Report):
    def __init__(self, datadir):
        Report.__init__(self, datadir)
        self.filename = "%s/report.txt" % self.rdir
    
    def write(self):
        self.start = timer2()
        self.out = open(self.filename, "w")
        
        self.db.agg_thread(True)
        self.db.agg_host(True)
        self.db.agg_all(True)

        # runtime summary
        runtime = self.runtime_stats()
        self.out.write("ParaMark: v%s, %s\n" 
            % (runtime["version"], runtime["date"]))
        self.out.write("Platform: %s\n" % runtime["platform"])
        self.out.write("Target: %s (%s)\n" % (runtime["wdir"],
            runtime["mountpoint"]))
        self.out.write("Time: %s --- %s (%.2f seconds)\n" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"]))))
        self.out.write("User: %s (%s)\n" % (runtime["user"], runtime["uid"]))
        self.out.write("Command: %s\n" % runtime["cmdline"])
        self.out.write("Configuration: ./fsbench.conf\n")
        self.out.write("Data: ./fsbench.db\n") 

        meta_opers = []
        meta_aggs = []
        meta_stds = []
        io_opers = []
        io_aggs = []
        io_stds = []
        for tid,op,optype,thmin,thmax,thavg,thagg,thstd \
            in self.db.get_stat_all():
            if optype == TYPE_META:
                meta_opers.append(op)
                meta_aggs.append(thagg)
                meta_stds.append(thstd)
            elif optype == TYPE_IO:
                io_opers.append(op)
                io_aggs.append(thagg)
                io_stds.append(thstd)
            
        if len(meta_aggs) > 0:
            meta_aggs = map(lambda x:str(x), meta_aggs)
            meta_stds = map(lambda x:str(x), meta_stds)
            self.out.write("\nMetadata Performance (ops/sec)\n")
            self.out.write("Oper: " + ",".join(meta_opers) + "\n")
            self.out.write("Aggs: " + ",".join(meta_aggs) + "\n")
            self.out.write("Stds: " + ",".join(meta_stds) + "\n")
        
        if len(io_aggs) > 0:
            io_aggs = map(lambda x:str(x/1048576), io_aggs)
            io_stds = map(lambda x:str(x/1048576), io_stds)
            self.out.write("\nI/O Performance (MB/sec)\n")
            self.out.write("Oper: " + ",".join(io_opers) + "\n")
            self.out.write("Aggs: " + ",".join(io_aggs) + "\n")
            self.out.write("Stds: " + ",".join(io_stds) + "\n")
        
        self.out.close()
        sys.stdout.write("Report generated to %s/report.txt\n" % self.rdir)

class HTMLReport(Report):
    def __init__(self, datadir):
        Report.__init__(self, datadir)
        import modules.plot as plot
        self.pyplot = plot.Pyplot()
        self.gplot = plot.GnuPlot(self.fdir)
        
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
        self.MAIN_FILE = "report.html"
        self.CSS_FILE = "report.css"
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
        
        self.runtime_section(doc, body)

        # I/O Section
        body.appendChild(doc.H(self.SECTION_SIZE, "I/O Performance"))
        body.appendChild(doc.H(self.SUBSECTION_SIZE, "Write"))
        tHead = [["fsize", "bsize", "agg", "agg w/o close()",
            "opAvg", "opMin", "opMax", "opStd", "opDist", "elasped",
            "accAgg"]]
        rows = []
        unit_suffix = "/sec"
        for hid,pid,tid,fsize,bsize,elapsed,agg,aggnoclose,opavg,opmin, \
            opmax,opstd in self.db.select_rawdata_all("write"):
            # figure generation
            op_unit, op_unit_val = unit_size(opavg)
            opdist = map(lambda e:bsize/e/op_unit_val, elapsed[1:-1])
            figname = "opdist_write_%s_%s_%s_%s_%s.png" % \
                (hid, pid, tid, fsize, bsize)
            self.gplot.impulse_chart(data=opdist, name=figname,
                title="Distribution of Per-Operation Throughput",
                xlabel="write() system call", 
                ylabel="Throughput (%s/sec)" % op_unit)
            figlink = "figures/%s" % figname
            opdist_fighref = doc.HREF(doc.IMG(figlink, 
                attrs={"class":"thumbnail"}), figlink)
            
            elap_unit, elap_unit_val = unit_time(num.average(elapsed))
            ylog = False
            if num.max(elapsed) / num.min(elapsed) > LOGSCALE_THRESHOLD:
                ylog = True
            figname = "elapsed_write_%s_%s_%s_%s_%s.png" % \
                (hid, pid, tid, fsize, bsize)
            self.gplot.impulse_chart(
                data=map(lambda e:e/elap_unit_val, elapsed), 
                name=figname,
                title="Distribution of System Call Latency",
                xlabel="System call",
                ylabel="Latency (%s)" % elap_unit,
                ylog=ylog)
            figlink = "figures/%s" % figname
            elapsed_fighref = doc.HREF(doc.IMG(figlink, 
                attrs={"class":"thumbnail"}), figlink)
            
            accagg_unit, accagg_unit_val = unit_size(agg)
            figname = "accagg_write_%s_%s_%s_%s_%s.png" % \
                (hid, pid, tid, fsize, bsize)
            t_bytes = 0
            t_elapsed = elapsed[0]
            accagg = [0.0] # open()
            for e in elapsed[1:-1]:
                t_bytes += bsize
                t_elapsed += e
                accagg.append(t_bytes / t_elapsed)
            # TODO: calculate when fsync is set
            accagg.append(t_bytes / (t_elapsed + elapsed[-1])) # close()
            self.gplot.impulse_chart(
                data=map(lambda a:a/accagg_unit_val, accagg),
                name=figname,
                title="Accumulated Aggregated Write Performance",
                xlabel="System Call",
                ylabel="Throughput (%s/sec)" % accagg_unit)
            figlink = "figures/%s" % figname
            accagg_fighref = doc.HREF(doc.IMG(figlink, 
                attrs={"class":"thumbnail"}), figlink)
            
            # unit conversion 
            fsize = unit_str(fsize)
            bsize = unit_str(bsize)
            agg = unit_str(agg, unit_suffix)
            aggnoclose = unit_str(aggnoclose, unit_suffix)
            opavg = unit_str(opavg, unit_suffix)
            opmin = unit_str(opmin, unit_suffix)
            opmax = unit_str(opmax, unit_suffix)
            opstd = unit_str(opstd, unit_suffix)

            rows.append([fsize,bsize,agg,aggnoclose,opavg,opmin,opmax,opstd,
                opdist_fighref,elapsed_fighref,accagg_fighref])
        body.appendChild(doc.table(tHead, rows))

        # footnote
        self.end = timer2()
        pNode = doc.tag("p", 
            value="Took %.2f seconds, styled by " 
                % ((self.end[1] - self.start[1])), attrs={"class":"footnote"})
        pNode.appendChild(doc.HREF(self.CSS_FILE, "./%s" % self.CSS_FILE))
        pNode.appendChild(doc.TEXT(", created by "))
        pNode.appendChild(doc.HREF("ParaMark v%s" % version.PARAMARK_VERSION,
            version.PARAMARK_WEB))
        pNode.appendChild(doc.TEXT(" at %s." %
            time.strftime("%a %b %d %Y %H:%M:%S %Z", self.end[0])))
        body.appendChild(pNode)
        
        htmlFile = open("%s/%s" % (self.rdir, self.MAIN_FILE), "w")
        doc.write(htmlFile)
        htmlFile.close()

    def runtime_section(self, doc, body):
        verbose("Generating \"Runtime Summary\" ...", REPORT_VERBOSE_LEVEL)
        body.appendChild(doc.H(self.SECTION_SIZE, "Runtime Summary"))
        runtime = self.runtime_stats()
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

    def css_file(self):
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARAMARK_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
        
    def write(self):
        self.start = timer2()
        self.css_file()
        self.main_page()
        message("Report generated in %s" % self.rdir)

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
font-size: 14pt;
}

H2 {
font-family: Arial;
font-size: 12pt;
background-color: #99c68e;
}

H3 {
font-family: Arial;
font-size: 12pt;
}

P[class=footnote] {
font-family: Times New Roman;
font-size: 10pt;
font-style: italic;
display: block;
background-color: #C3FDB8;
}

IMG[class=thumbnail] {
border-style: groove;
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
font-size: 12px;
border-collapse: collapse;
width: 100%;
}

TH {
font-size: 12px;
font-weight: bold;
padding: 6px 8px;
border-bottom: 1px solid;
vertical-align: baseline;
text-align: left;
}

TD {
padding: 2px 2px;
text-align: left;
}

UL[class=navi] {
font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;
font-size: 14px;
cursor: pointer;
}
"""
