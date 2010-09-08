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
import csv
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

VERBOSE = 2
VERBOSE_MORE = VERBOSE + 1
VERBOSE_ALL = VERBOSE_MORE + 1

class Report():
    def __init__(self, datadir, db, cfg):
        self.datadir = os.path.abspath(datadir)
        if db is None:
            self.db = data.Database("%s/fsbench.db" % self.datadir)
        else: self.db = db
        self.cfg = cfg
        
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

    def runtime_vals(self):
        runtime = {}
        for k, v in self.db.get_runtimes(): runtime[k] = v
        res = []
        res.append(("ParaMark", "v%s, %s" 
            % (runtime["version"], runtime["date"])))
        res.append(("Platform", "%s" % runtime["platform"]))
        res.append(("Target", "%s (%s)" 
            % (runtime["wdir"], runtime["mountpoint"])))
        res.append(("Time", "%s --- %s (%.2f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"])))))
        res.append(("User", "%s (%s)" % (runtime["user"], runtime["uid"])))
        res.append(("Command", "%s" % runtime["cmdline"]))
        if not self.cfg.nolog:
            res.append(("Configuration", "../fsbench.conf"))
            res.append(("Data", "../fsbench.db"))
        return res

    def meta_thread_vals(self, oper, hid, unit='auto', figure=False):
        rows = []
        for hid,pid,tid,opcnt,factor,elapsed,_,agg, \
            opavg,opmin,opmax,opstd in \
            self.db.select_rawdata_all(oper):
            if figure:
                opdist = map(lambda e:1/e, elapsed)
                opdist_figname = "opdist_%s_%d_%d_%d_%d_%d.png" % \
                    (oper, hid, pid, tid, opcnt, factor)
                self.gplot.impulse_chart(data=opdist,
                    name=opdist_figname,
                    title="Distribution of Per-Operation Throughput",
                    xlabel="%s() system call" % oper,
                    ylabel="Throughput (ops/sec)")
                
                elap_unit, elap_unit_val = unit_time(num.average(elapsed))
                ylog = False
                if num.max(elapsed) / num.min(elapsed) > LOGSCALE_THRESHOLD:
                    ylog = True
                elapsed_figname = "elapsed_%s_%d_%d_%d_%d_%d.png" % \
                    (oper, hid, pid, tid, opcnt, factor)
                self.gplot.impulse_chart(
                    data=map(lambda e:e/elap_unit_val, elapsed),
                    name=elapsed_figname,
                    title="Distribution of System Call Latency",
                    xlabel="%s system call" % oper,
                    ylabel="Latency (%s)" % elap_unit,
                    ylog=ylog)
                
                t_opcnt = 0
                t_elapsed = 0
                accagg = []
                for e in elapsed:
                    t_opcnt += 1
                    t_elapsed += e
                    accagg.append(t_opcnt / t_elapsed)
                accagg_figname = "accagg_%s_%d_%d_%d_%d_%d.png" % \
                    (oper, hid, pid, tid, opcnt, factor)
                self.gplot.impulse_chart(data=accagg,
                    name=accagg_figname,
                    title="Accumulated Aggregated Performance",
                    xlabel="%s System Call" % oper,
                    ylabel="Throughput (ops/sec)")

            if unit == 'auto': 
                agg = "%s ops/s" % round(agg, 3)
                opavg = "%s ops/s" % round(opavg, 3)
                opmin = "%s ops/s" % round(opmin, 3)
                opmax = "%s ops/s" % round(opmax, 3)
                opstd = "%s ops/s" % round(opstd, 3)
            
            row = [oper,hid,tid,opcnt,factor,agg,opavg,opmin,opmax,opstd]
            if figure:
                row.extend([opdist_figname,elapsed_figname,accagg_figname])
            rows.append(row)
        return rows

    def meta_host_vals(self, oper, hid, unit='auto', figure=False):
        rows = []
        res = Table()
        for _,_,tid,opcnt,factor,_,synctime,agg,_,_,_,_ in \
            self.db.select_rawdata_hid(oper, hid):
            r = res.get(opcnt, factor)
            if r is None:
                thdaggs = []
                syncs = []
            else: thdaggs, syncs = r
            thdaggs.append(agg)
            syncs.append(synctime)
            res.set(opcnt, factor, (thdaggs, syncs))

        for oc in res.get_rows():
            for ft in res.get_cols():
                thdaggs, syncs = res.get(oc, ft)
                agg = oc * len(thdaggs) / num.average(syncs)
                thdavg = num.average(thdaggs)
                thdmin = num.min(thdaggs)
                thdmax = num.max(thdaggs)
                thdstd = num.std(thdaggs)
                
                # figure generation
                if figure:
                    thddist_figname = "thddist_%s_%d_%d_%d.png" % \
                        (oper, hid, oc, ft)
                    self.gplot.impulse_chart(data=thdaggs,
                        name=thddist_figname,
                        title="Distribution of Per-Thread Throughput",
                        xlabel="Thread", 
                        ylabel="%s Throughput (ops/sec)" % oper,
                        xmin=-1, xmax=len(thdaggs))

                if unit == 'auto': 
                    agg = "%s ops/s" % round(agg, 3)
                    thdavg = "%s ops/s" % round(thdavg, 3)
                    thdmin = "%s ops/s" % round(thdmin, 3)
                    thdmax = "%s ops/s" % round(thdmax, 3)
                    thdstd = "%s ops/s" % round(thdstd, 3)
                
                row = [oper,hid,oc,ft,agg,thdavg,thdmin,thdmax,thdstd]
                if figure: row.append(thddist_figname)
                rows.append(row)

        return rows

    def meta_all_vals(self, oper, unit='auto', figure=False):
        rows = []
        res = Table()
        for _,_,tid,opcnt,factor,_,synctime,agg,_,_,_,_ in \
            self.db.select_rawdata_all(oper):
            r = res.get(opcnt, factor)
            if r is None:
                thdaggs = []
                syncs = []
            else: thdaggs, syncs = r
            thdaggs.append(agg)
            syncs.append(synctime)
            res.set(opcnt, factor, (thdaggs, syncs))

        for oc in res.get_rows():
            for ft in res.get_cols():
                thdaggs, syncs = res.get(oc, ft)
                agg = oc * len(thdaggs) / num.average(syncs)
                thdavg = num.average(thdaggs)
                thdmin = num.min(thdaggs)
                thdmax = num.max(thdaggs)
                thdstd = num.std(thdaggs)
                
                # figure generation
                if figure:
                    thddist_figname = "thddist_%s_all_%d_%d.png" % \
                        (oper, oc, ft)
                    self.gplot.impulse_chart(data=thdaggs,
                        name=thddist_figname,
                        title="Distribution of Per-Thread Throughput",
                        xlabel="Thread", 
                        ylabel="%s Throughput (ops/sec)" % oper,
                        xmin=-1, xmax=len(thdaggs))
                
                if unit == 'auto':
                    agg = "%s ops/s" % round(agg, 3)
                    thdavg = "%s ops/s" % round(thdavg, 3)
                    thdmin = "%s ops/s" % round(thdmin, 3)
                    thdmax = "%s ops/s" % round(thdmax, 3)
                    thdstd = "%s ops/s" % round(thdstd, 3)
                
                row = [oper,oc,ft,agg,thdavg,thdmin,thdmax,thdstd]
                if figure: row.append(thddist_figname)
                rows.append(row)

        return rows
        
    def io_thread_vals(self, oper, hid, unit='auto', figure=False):
        rows = []
        unit_suffix = "/s"
        for _,pid,tid,fsize,bsize,elapsed,_,agg,aggnoclose, \
            opavg,opmin,opmax,opstd in \
            self.db.select_rawdata_hid(oper, hid):
            # figure generation
            if figure:
                if unit == 'auto':
                    op_unit, op_unit_val = unit_size(opavg)
                opdist = map(lambda e:bsize/e/op_unit_val, elapsed[1:-1])
                opdist_figname = "opdist_%s_%s_%s_%s_%s_%s.png" % \
                    (oper, hid, pid, tid, fsize, bsize)
                self.gplot.impulse_chart(data=opdist, 
                    name=opdist_figname,
                    title="Distribution of Per-Operation Throughput",
                    xlabel="%s() system call" % oper, 
                    ylabel="Throughput (%s/sec)" % op_unit)
                
                if unit == 'auto':
                    elap_unit, elap_unit_val = unit_time(num.average(elapsed))
                ylog = False
                if num.max(elapsed) / num.min(elapsed) > \
                    LOGSCALE_THRESHOLD:
                    ylog = True
                elapsed_figname = "elapsed_%s_%s_%s_%s_%s_%s.png" % \
                    (oper, hid, pid, tid, fsize, bsize)
                self.gplot.impulse_chart(
                    data=map(lambda e:e/elap_unit_val, elapsed), 
                    name=elapsed_figname,
                    title="Distribution of System Call Latency",
                    xlabel="System call",
                    ylabel="Latency (%s)" % elap_unit,
                    ylog=ylog)
                
                if unit == 'auto':
                    accagg_unit, accagg_unit_val = unit_size(agg)

                accagg_figname = "accagg_%s_%s_%s_%s_%s_%s.png" % \
                    (oper, hid, pid, tid, fsize, bsize)
                t_bytes = 0
                t_elapsed = elapsed[0]
                accagg = [0.0] # open()
                for e in elapsed[1:-1]:
                    t_bytes += bsize
                    t_elapsed += e
                    accagg.append(t_bytes / t_elapsed)
                accagg.append(t_bytes / (t_elapsed + elapsed[-1]))
                self.gplot.impulse_chart(
                    data=map(lambda a:a/accagg_unit_val, accagg),
                    name=accagg_figname,
                    title="Accumulated Aggregated Performance",
                    xlabel="%s System Call" % oper,
                    ylabel="Throughput (%s/sec)" % accagg_unit)
            
            # unit conversion 
            if unit == 'auto':
                fsize = unit_str(fsize)
                bsize = unit_str(bsize)
                agg = unit_str(agg, unit_suffix)
                aggnoclose = unit_str(aggnoclose, unit_suffix)
                opavg = unit_str(opavg, unit_suffix)
                opmin = unit_str(opmin, unit_suffix)
                opmax = unit_str(opmax, unit_suffix)
                opstd = unit_str(opstd, unit_suffix)
            
            if figure:
                rows.append([oper,hid,tid,fsize,bsize,agg,aggnoclose,
                    opavg,opmin,opmax,opstd,
                    opdist_figname,elapsed_figname,accagg_figname])
            else:
                rows.append([oper,hid,tid,fsize,bsize,agg,aggnoclose,
                    opavg,opmin,opmax,opstd])

        return rows

    def io_host_vals(self, oper, hid, unit='auto', figure=False):
        unit_suffix = "/s"
        rows = []
        res = Table()
        for _,_,tid,fsize,bsize,_,synctime,agg,_,_,_,_,_ in \
            self.db.select_rawdata_hid(oper, hid):
            r = res.get(fsize, bsize)
            if r is None:
                thdaggs = []
                syncs = []
            else: thdaggs, syncs = r
            thdaggs.append(agg)
            syncs.append(synctime)
            res.set(fsize, bsize, (thdaggs, syncs))

        for fs in res.get_rows():
            for bs in res.get_cols():
                thdaggs, syncs = res.get(fs, bs)
                agg = fs * len(thdaggs) / num.average(syncs)
                thdavg = num.average(thdaggs)
                thdmin = num.min(thdaggs)
                thdmax = num.max(thdaggs)
                thdstd = num.std(thdaggs)
                
                # figure generation
                if figure:
                    thd_unit, thd_unit_val = unit_size(thdavg)
                    thddist = map(lambda t:t/thd_unit_val, thdaggs)
                    thddist_figname = "thddist_%s_%d_%d_%d.png" % \
                        (oper, hid, fs, bs)
                    self.gplot.impulse_chart(data=thddist,
                        name=thddist_figname,
                        title="Distribution of Per-Thread Throughput",
                        xlabel="Thread", 
                        ylabel="%s Throughput (%s/sec)" \
                            % (oper, thd_unit),
                        xmin=-1, xmax=len(thdaggs))

                # unit conversion
                if unit == 'auto':
                    fs = unit_str(fs)
                    bs = unit_str(bs)
                    agg = unit_str(agg, unit_suffix)
                    thdavg = unit_str(thdavg, unit_suffix)
                    thdmin = unit_str(thdmin, unit_suffix)
                    thdmax = unit_str(thdmax, unit_suffix)
                    thdstd = unit_str(thdstd, unit_suffix)
                
                row = [oper,hid,fs,bs,agg,thdavg,thdmin,thdmax,thdstd]
                if figure: row.append(thddist_figname)
                rows.append(row)
        
        return rows
    
    def io_all_vals(self, oper, unit='auto', figure=False):
        unit_suffix = "/s"
        rows = []
        res = Table()
        for _,_,tid,fsize,bsize,_,synctime,agg,_,_,_,_,_ in \
            self.db.select_rawdata_all(oper):
            r = res.get(fsize, bsize)
            if r is None:
                thdaggs = []
                syncs = []
            else: thdaggs, syncs = r
            thdaggs.append(agg)
            syncs.append(synctime)
            res.set(fsize, bsize, (thdaggs, syncs))

        for fs in res.get_rows():
            for bs in res.get_cols():
                thdaggs, syncs = res.get(fs, bs)
                agg = fs * len(thdaggs) / num.average(syncs)
                thdavg = num.average(thdaggs)
                thdmin = num.min(thdaggs)
                thdmax = num.max(thdaggs)
                thdstd = num.std(thdaggs)
                
                # figure generation
                if unit == 'auto':
                    thd_unit, thd_unit_val = unit_size(thdavg)
                
                if figure:
                    thddist = map(lambda t:t/thd_unit_val, thdaggs)
                    thddist_figname = "thddist_%s_all_%d_%d.png" % \
                        (oper, fs, bs)
                    self.gplot.impulse_chart(data=thddist,
                        name=thddist_figname,
                        title="Distribution of Per-Thread Throughput",
                        xlabel="Thread", 
                        ylabel="%s Throughput (%s/sec)" % (oper, thd_unit),
                        xmin=-1, xmax=len(thdaggs))

                # unit conversion
                if unit == 'auto':
                    fs = unit_str(fs)
                    bs = unit_str(bs)
                    agg = unit_str(agg, unit_suffix)
                    thdavg = unit_str(thdavg, unit_suffix)
                    thdmin = unit_str(thdmin, unit_suffix)
                    thdmax = unit_str(thdmax, unit_suffix)
                    thdstd = unit_str(thdstd, unit_suffix)
                
                row = [oper,fs,bs,agg,thdavg,thdmin,thdmax,thdstd]
                if figure: row.append(thddist_figname)
                rows.append(row)
    
        return rows
                
class TextReport(Report):
    def __init__(self, datadir, db, cfg):
        Report.__init__(self, datadir, db, cfg)
        self.filename = "%s/report.txt" % self.rdir
        self.f = None

    def runtime_section(self):
        self.f.write("# Runtime Summary\n")
        for v in self.runtime_vals(): self.f.write("%s: %s\n" % v)
        self.f.write("\n")
        self.f.flush()

    def meta_section(self):
        section_order = ["mkdir", "rmdir", "creat", "access", 
            "open", "open_close", "stat_exist", "stat_non", "utime", 
            "chmod", "rename", "unlink"]
        opers = sorted(list_intersect([OPS_META, self.db.get_tables()]), 
            key=lambda t:section_order.index(t))
        if len(opers) == 0: return
        verbose(" writing \"Metadata Section\" ...", VERBOSE_MORE)
        self.f.write("# Metadata Performance\n")
        hids = self.db.get_hids(opers[0])
        pids = self.db.get_pids(opers[0])
        tids = self.db.get_tids(opers[0])
        if len(hids) > 1 or len(tids) > 1: self.meta_all_report(opers)
        if len(hids) > 1: self.meta_host_report(opers, hids)
        if len(hids) >= 1 or len(tids) > 1:
            self.meta_thread_report(opers, hids)
        self.f.flush()

    def meta_thread_report(self, opers, hids):
        self.f.write("Meta:Per-Thread Performance\n")
        rows = [["oper", "hid", "tid", "opcnt", "factor", "agg", 
            "opAvg", "opMin", "opMax", "opStd"]]
        for oper in opers:
            for hid in hids: rows.extend(self.meta_thread_vals(oper, hid))
        print_text_table(self.f, rows)
        self.f.write("\n")
        self.f.flush()

    def meta_host_report(self, opers, hids):
        self.f.write("Meta:Per-Host Performance\n")
        rows = [["oper", "hid", "opcnt", "factor", "agg", 
            "opAvg", "opMin", "opMax", "opStd"]]
        for oper in opers:
            for hid in hids: rows.extend(self.meta_host_vals(oper, hid))
        print_text_table(self.f, rows)
        self.f.write("\n")
        self.f.flush()

    def meta_all_report(self, opers):
        self.f.write("Meta:Per-Thread Performance\n")
        rows = [["oper", "opcnt", "factor", "agg", 
            "opAvg", "opMin", "opMax", "opStd"]]
        for oper in opers: rows.extend(self.meta_all_vals(oper))
        print_text_table(self.f, rows)
        self.f.write("\n")
        self.f.flush()
    
    def io_section(self):
        opers = sorted(list_intersect([OPS_IO, self.db.get_tables()]), 
            key=lambda t:OPS_IO.index(t))
        if len(opers) == 0: return
        verbose(" writing \"I/O Section\" ...", VERBOSE_MORE)
        self.f.write("# I/O Performance\n")
        hids = self.db.get_hids(opers[0])
        pids = self.db.get_pids(opers[0])
        tids = self.db.get_tids(opers[0])
        if len(hids) > 1 or len(tids) > 1: self.io_all_report(opers)
        if len(hids) > 1: self.io_host_report(opers, hids)
        if len(hids) >= 1 or len(tids) > 1:
            self.io_thread_report(opers, hids)
        self.f.flush()

    def io_thread_report(self, opers, hids):
        self.f.write("IO:Per-Thread Performance\n")
        rows = [["oper", "hid", "tid", "fsize", "bsize", "agg", 
            "agg w/o close()", "opAvg", "opMin", "opMax", "opStd"]]
        for oper in opers:
            for hid in hids: rows.extend(self.io_thread_vals(oper, hid))
        print_text_table(self.f, rows)
        self.f.write("\n")
        self.f.flush()
    
    def io_host_report(self, opers, hids):
        self.f.write("IO:Per-Host Performance\n")
        rows = [["oper", "hid", "fsize", "bsize", "agg", "thdAvg", "thdMin", 
            "thdMax", "thdStd"]]
        for oper in opers:
            for hid in hids: rows.extend(self.io_host_vals(oper, hid))
        print_text_table(self.f, rows)
        self.f.write("\n")
        self.f.flush()

    def io_all_report(self, opers):
        self.f.write("IO:Overall Performance\n")
        rows = [["oper", "fsize", "bsize", "agg", "thdAvg", "thdMin",
            "thdMax", "thdStd"]]
        for oper in opers: rows.extend(self.io_all_vals(oper))
        print_text_table(self.f, rows)
        self.f.write("\n")
        self.f.flush()
    
    def write(self):
        self.start = timer2()
        if self.cfg.quickreport:
            message("Generating text report ...")
            self.f = sys.stdout
        else:
            message("Generating text report to %s/report.txt ..." % self.rdir)
            self.f = open(self.filename, "w")
        
        self.runtime_section()
        self.meta_section()
        self.io_section()
        
        if self.cfg.quickreport:
            self.f.flush()
        else:
            self.f.close()
            message("Done!")

class HTMLReport(Report):
    def __init__(self, datadir, db, cfg):
        Report.__init__(self, datadir, db, cfg)
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
        self.meta_section(doc, body)
        self.io_section(doc, body)
        self.footnote_section(doc, body)

    def runtime_section(self, doc, body):
        verbose(" writing \"Runtime Summary\" ...", VERBOSE_MORE)
        body.appendChild(doc.H(self.SECTION_SIZE, "Runtime Summary"))
        rows = []
        for item, val in self.runtime_vals():
            if item in ["Configuration", "Data"]:
                val = doc.HREF(os.path.basename(val), val)
            rows.append([item, val])
        body.appendChild(doc.table([], rows))

    def footnote_section(self, doc, body):
        self.end = timer2()
        pNode = doc.tag("p", value="Took %.2f seconds, styled by " 
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

    def io_section(self, doc, body):
        opers = sorted(list_intersect([OPS_IO, self.db.get_tables()]), 
            key=lambda t:OPS_IO.index(t))
        if len(opers) == 0: return
        verbose(" writing \"I/O Section\" ...", VERBOSE_MORE)
        body.appendChild(doc.H(self.SECTION_SIZE, "I/O Performance"))
        hids = self.db.get_hids(opers[0])
        pids = self.db.get_pids(opers[0])
        tids = self.db.get_tids(opers[0])
        if len(hids) > 1 or len(tids) > 1:
            self.io_all_report(opers, doc, body)
        if len(hids) > 1:
            self.io_host_report(opers, hids, doc, body)
        if len(hids) >= 1 or len(tids) > 1:
            self.io_thread_report(opers, hids, doc, body)
            
    def io_thread_report(self, opers, hids, doc, body):
        body.appendChild(doc.H(self.SUBSECTION_SIZE, 
            "Per-Thread Performance"))
        tHead = [["oper", "hid", "tid", "fsize", "bsize", "agg", 
            "agg w/o close()", "opAvg", "opMin", "opMax", "opStd", 
            "opDist", "elasped", "accAgg"]]
        rows = []
        for oper in opers:
            for hid in hids:
                for res in self.io_thread_vals(oper, hid, 'auto', True):
                    for i in range(-3,0):
                        figlink = "figures/%s" % res[i]
                        res[i] = doc.HREF(doc.IMG(figlink,
                            attrs={"class":"thumbnail"}), figlink)
                    rows.append(res)
        body.appendChild(doc.table(tHead, rows))

    def io_host_report(self, opers, hids, doc, body):
        verbose(" writing I/O host performance report ...", VERBOSE_ALL)
        body.appendChild(doc.H(self.SUBSECTION_SIZE, 
            "Per-Host Performance"))
        tHead = [["oper", "hid", "fsize", "bsize", "agg", \
            "thdAvg", "thdMin", "thdMax", "thdStd", "thdDist"]]
        rows = []
        for oper in opers:
            for hid in hids:
                for res in self.io_host_vals(oper, hid, 'auto', True):
                    figlink = "figures/%s" % res[-1]
                    res[-1] = doc.HREF(doc.IMG(figlink,
                        attrs={"class":"thumbnail"}), figlink)
                    rows.append(res)
        body.appendChild(doc.table(tHead, rows))
    
    def io_all_report(self, opers, doc, body):
        verbose(" writing I/O overall performance report ...", VERBOSE_ALL)
        body.appendChild(doc.H(self.SUBSECTION_SIZE, 
            "Overall Performance"))
        tHead = [["oper", "fsize", "bsize", "agg", "thdAvg", "thdMin", \
            "thdMax", "thdStd", "thdDist"]]
        rows = []
        for oper in opers:
            for res in self.io_all_vals(oper, 'auto', True):
                figlink = "figures/%s" % res[-1]
                res[-1] = doc.HREF(doc.IMG(figlink,
                    attrs={"class":"thumbnail"}), figlink)
                rows.append(res)
        body.appendChild(doc.table(tHead, rows))
            
    def meta_section(self, doc, body):
        section_order = ["mkdir", "rmdir", "creat", "access", 
            "open", "open_close", "stat_exist", "stat_non", "utime", 
            "chmod", "rename", "unlink"]
        opers = sorted(list_intersect([OPS_META, self.db.get_tables()]), 
            key=lambda t:section_order.index(t))
        if len(opers) == 0: return
        verbose(" writing \"Metadata Section\" ...", VERBOSE_MORE)
        body.appendChild(doc.H(self.SECTION_SIZE, "Metadata Performance"))
        hids = self.db.get_hids(opers[0])
        pids = self.db.get_pids(opers[0])
        tids = self.db.get_tids(opers[0])
        if len(hids) > 1 or len(tids) > 1:
            self.meta_all_report(opers, doc, body)
        if len(hids) > 1: self.meta_host_report(opers, hids, doc, body)
        if len(hids) >= 1 or len(tids) > 1:
            self.meta_thread_report(opers, hids, doc, body)
    
    def meta_thread_report(self, opers, hids, doc, body):
        body.appendChild(doc.H(self.SUBSECTION_SIZE, 
            "Per-Thread Performance"))
        tHead = [["oper", "hid", "tid", "opcnt", "factor", "agg", "opAvg", 
            "opMin", "opMax", "opStd", "opDist", "elasped", "accAgg"]]
        rows = []
        for oper in opers:
            for hid in hids:
                for res in self.meta_thread_vals(oper, hid, 'auto', True):
                    for i in range(-3,0):
                        figlink = "figures/%s" % res[i]
                        res[i] = doc.HREF(doc.IMG(figlink,
                            attrs={"class":"thumbnail"}), figlink)
                    rows.append(res)
        body.appendChild(doc.table(tHead, rows))
    
    def meta_host_report(self, opers, hids, doc, body):
        verbose(" writing metadata host performance report ...", 
            VERBOSE_ALL)
        body.appendChild(doc.H(self.SUBSECTION_SIZE, 
            "Per-Host Performance"))
        tHead = [["oper", "hid", "opcnt", "factor", "agg", "thdAvg", 
            "thdMin", "thdMax", "thdStd", "thdDist"]]
        rows = []
        for oper in opers:
            for hid in hids:
                for res in self.meta_host_vals(oper, hid, 'auto', True):
                    figlink = "figures/%s" % res[-1]
                    res[-1] = doc.HREF(doc.IMG(figlink,
                        attrs={"class":"thumbnail"}), figlink)
                    rows.append(res)
        body.appendChild(doc.table(tHead, rows))
        
    def meta_all_report(self, opers, doc, body):
        verbose(" writing metadata overall performance report ...", 
            VERBOSE_ALL)
        body.appendChild(doc.H(self.SUBSECTION_SIZE, 
            "Overall Performance"))
        tHead = [["oper", "opcnt", "factor", "agg", "thdAvg", 
            "thdMin", "thdMax", "thdStd", "thdDist"]]
        rows = []
        for oper in opers:
            for res in self.meta_all_vals(oper, 'auto', True):
                figlink = "figures/%s" % res[-1]
                res[-1] = doc.HREF(doc.IMG(figlink,
                    attrs={"class":"thumbnail"}), figlink)
                rows.append(res)
        body.appendChild(doc.table(tHead, rows))

    def css_file(self):
        verbose(" saving css style file to %s/%s ..." % 
            (self.rdir, self.CSS_FILE))
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARAMARK_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
        
    def write(self):
        self.start = timer2()
        message("Generating HTML report to %s ... " % self.rdir)
        self.main_page()
        self.css_file()
        message("Done!")

class CSVReport(Report):
    def __init__(self, datadir, db, cfg):
        Report.__init__(self, datadir, db, cfg)
    
    def runtime_report(self):
        verbose(" writing runtim csv report ...", VERBOSE_ALL)
        f = open("%s/runtime.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerows(self.runtime_vals())
        f.close()

    def meta_report(self):
        section_order = ["mkdir", "rmdir", "creat", "access", 
            "open", "open_close", "stat_exist", "stat_non", "utime", 
            "chmod", "rename", "unlink"]
        opers = sorted(list_intersect([OPS_META, self.db.get_tables()]), 
            key=lambda t:section_order.index(t))
        if len(opers) == 0: return
        verbose(" writing metadata csv report ...", VERBOSE_MORE)
        hids = self.db.get_hids(opers[0])
        pids = self.db.get_pids(opers[0])
        tids = self.db.get_tids(opers[0])
        if len(opers) == 0: return
        if len(hids) > 1 or len(tids) > 1: self.meta_all_report(opers)
        if len(hids) > 1: self.meta_host_report(opers, hids)
        if len(hids) >= 1 or len(tids) > 1:
            self.meta_thread_report(opers, hids)
    
    def meta_thread_report(self, opers, hids):
        verbose(" writing metadata per-thread csv report ...", VERBOSE_ALL)
        f = open("%s/meta_thread.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerow(["oper", "hid", "tid", "opcnt", "factor", "agg", 
            "opAvg", "opMin", "opMax", "opStd"])
        for oper in opers:
            for hid in hids:
                csvw.writerows(self.meta_thread_vals(oper, hid, None))
        f.close()

    def meta_host_report(self, opers, hids):
        verbose(" writing metadata per-host csv report ...", VERBOSE_ALL)
        f = open("%s/meta_host.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerow(["oper", "hid", "opcnt", "factor", "agg", 
            "opAvg", "opMin", "opMax", "opStd"])
        for oper in opers:
            for hid in hids:
                csvw.writerows(self.meta_host_vals(oper, hid, None))
        f.close()
    
    def meta_all_report(self, opers):
        verbose(" writing metadata overall csv report ...", VERBOSE_ALL)
        f = open("%s/meta_overall.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerow(["oper", "opcnt", "factor", "agg", 
            "opAvg", "opMin", "opMax", "opStd"])
        for oper in opers:
            csvw.writerows(self.meta_all_vals(oper, None))
        f.close()
    
    def io_report(self):
        opers = sorted(list_intersect([OPS_IO, self.db.get_tables()]), 
            key=lambda t:OPS_IO.index(t))
        if len(opers) == 0: return
        verbose(" writing I/O csv report ...", VERBOSE_MORE)
        hids = self.db.get_hids(opers[0])
        pids = self.db.get_pids(opers[0])
        tids = self.db.get_tids(opers[0])
        if len(hids) > 1 or len(tids) > 1: self.io_all_report(opers)
        if len(hids) > 1: self.io_host_report(opers, hids)
        if len(hids) >= 1 or len(tids) > 1:
            self.io_thread_report(opers, hids)
    
    def io_thread_report(self, opers, hids):
        verbose(" writing I/O per-thread csv report ...", VERBOSE_ALL)
        f = open("%s/io_thread.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerow(["oper", "hid", "tid", "fsize", "bsize", "agg", 
            "agg w/o close()", "opAvg", "opMin", "opMax", "opStd"])
        for oper in opers:
            for hid in hids:
                csvw.writerows(self.io_thread_vals(oper, hid, None))
        f.close()
    
    def io_host_report(self, opers, hids):
        verbose(" writing I/O per-host csv report ...", VERBOSE_ALL)
        f = open("%s/io_host.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerow(["oper", "hid", "fsize", "bsize", "agg", "thdAvg", 
            "thdMin", "thdMax", "thdStd"])
        for oper in opers:
            for hid in hids:
                csvw.writerows(self.io_host_vals(oper, hid, None))
        f.close()

    def io_all_report(self, opers):
        verbose(" writing I/O overall csv report ...", VERBOSE_ALL)
        f = open("%s/io_overall.csv" % self.ddir, "wb")
        csvw = csv.writer(f)
        csvw.writerow(["oper", "fsize", "bsize", "agg", "thdAvg", "thdMin",
            "thdMax", "thdStd"])
        for oper in opers:
            csvw.writerows((self.io_all_vals(oper, None)))
        f.close()

    def write(self):
        message("Generating CSV report to %s ... " % self.ddir)
        self.runtime_report()
        self.meta_report()
        self.io_report()
        message("Done!")

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
font-family: monospace;
font-size: 11pt;
background-color: #efefef;
}

P[class=footnote] {
font-family: Times New Roman;
font-size: 10pt;
font-style: italic;
display: block;
background-color: #C3FDB8;
}

P[class=supplement] {
font-family: Times New Roman;
font-size: 10pt;
}

IMG[class=thumbnail] {
border-style: groove;
border-width: 1px;
width: 16px;
height: 16px;
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
font-size: 11px;
border-collapse: collapse;
width: 100%;
}

TH {
font-size: 11px;
font-weight: bold;
padding: 2px 2px;
border-bottom: 1px solid;
vertical-align: baseline;
text-align: left;
}

TD {
text-align: left;
}

UL[class=navi] {
font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;
font-size: 14px;
cursor: pointer;
}
"""
