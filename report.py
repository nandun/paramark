#############################################################################
# ParaMark: High Fidelity Parallel File System Benchmark
# Copyright (C) 2009  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
#
# This program is free sofmtware: you can redistribute it and/or modify
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

import csv
import os
import sqlite3
import sys
import time

from common import *
from database import SerialBenchmarkDB

class Report():
    def __init__(self, opts=None):
        self.unit = None
        self.chartdatalabels = None
        
        _SerialBenchmarkReport_restrict = ["unit", "chartdatalabels"]
        update_opts_kw(self.__dict__, _SerialBenchmarkReport_restrict,
            opts, None)
    
class SerialBenchmarkReport(Report, SerialBenchmarkDB):
    def __init__(self, dbfile, opts=None):
        Report.__init__(self, opts)
        SerialBenchmarkDB.__init__(self, dbfile)


        self.dbhome = os.path.dirname(dbfile)
    
    def __del__(self):
        self.close_database()

    def summary(self, stream=None):
        if stream is None:
            stream = sys.stdout
        
        env = {}
        for i,v in self.env_select():
            env[i] = v
        
        stream.write(\
"""
ParaMark Base Benchmark (version %s, %s)
          platform: %s
         run began: %s
           run end: %s
          duration: %s seconds
              user: %s (%s)
           command: %s
 working directory: %s
              mode: %s
""" \
        % (env["ParaMark Version"], env["ParaMark Date"],
           env["platform"],
           time.strftime("%a, %d %b %Y %H:%M:%S %Z", eval(env["run began"])),
           time.strftime("%a, %d %b %Y %H:%M:%S %Z", eval(env["run end"])),
           env["duration"],
           env["user"], env["uid"],
           env["command"],
           env["working directory"],
           env["mode"]))

        stream.flush()

    # output data in various format
    def toscreen(self, file=None):
        if file is None:
            file = sys.stdout
        
        self.summary(file)
        file.write("\nI/O Performance Data (%s/sec)\n" % self.unit)
        formatstr = "%10s%7s%10s%10s%20s%20s%20s%20s\n"
        file.write(formatstr % ("operation", "proc", "filesize", 
            "blocksize", "exectime", "mintime/call", "maxtime/call", 
            "throughput"))
        unit = eval(self.unit)
        for row in self.io_select():
            o, p, f, b, e, mi, mx, t = row
            f = "%s%s" % smart_datasize(f)
            b = "%s%s" % smart_datasize(b)
            t = "%s" % (t/unit)
            file.write(formatstr % (o, p, f, b, e, mi, mx, t))

        file.write("\nMetadata Performance Data (ops/sec)\n")
        formatstr = "%15s%7s%10s%10s%20s%20s%20s%20s\n"
        file.write(formatstr % ("operation", "proc", "count", 
            "factor", "exectime", "mintime/call", "maxtime/call", 
            "throughput"))
        for row in self.meta_select():
            file.write(formatstr % row)

    def tocsv(self):
        envf = open("%s/env.csv" % self.home, "wb")
        iof = open("%s/io.csv" % self.home, "wb")
        metaf = open("%s/meta.csv" % self.home, "wb")
        envcsv = csv.writer(envf)
        iocsv = csv.writer(iof)
        metacsv = csv.writer(metaf)
       
        envcsv.writerow(["enviromental variable", "value"])
        for row in self.db.env_select():
            envcsv.writerow(row)

        iocsv.writerow(["operation", "nproc", "filesize", "blocksize",
            "exectime", "mintime/call", "maxtime/call", "throughput"])
        unit = eval(self.unit)
        for row in self.db.ioselall():
            o, p, f, b, e, mi, mx, t = row
            f = "%s%s" % smart_datasize(f)
            b = "%s%s" % smart_datasize(b)
            t = "%s%s" % (t/unit, self.unit)
            iocsv.writerow((o, p, f, b, e, mi, mx, t))
        
        metacsv.writerow(["operation", "nproc", "count", "factor", "exectime",
            "mintime/call", "maxtime/call", "throughput"])
        for row in self.db.metaselall():
            metacsv.writerow(row)
        
        envf.close()
        iof.close()
        metaf.close()

        ws("\nEnvironment data have been written to %s/env.csv.\n" %
            self.home)
        ws("I/O performance data have been written to %s/io.csv.\n" %
            self.home)
        ws("Metadata performance data have been written to %s/meta.csv.\n" %
            self.home)
    
    def tohtml(self, file=None):
        import gchart
        if file is None:
            file = "%s/report.html" % self.dbhome
        
        modulesdir = "../modules"
        settingsdir = "../settings"
        datadir = "."
        basename = os.path.basename(self.dbhome)

        start = (time.localtime(), timer())

        # prepare data

        # generate html
        f = open(file, "wb")

        f.write(
"""\
<!-- 
  ParaMark Report
  by %s at %s
-->
""" % \
        (os.path.abspath(sys.argv[0]),
         time.strftime("%a %b %d %Y %H:%M:%S %Z", start[0])))
        
        f.write(
"""\
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<title>ParaMark Report: %s</title>
""" % time.strftime("%a %b %d %Y %H:%M:%S %Z", start[0]))
        
        env = {}
        for i,v in self.env_select():
            env[i] = v
        f.write(
"""\
<!-- Log: ParaMark runtime summary -->
<p align="left"><font size=5"><b>ParaMark Performance Report: %s</b></font><br>
<i>by ParaMark Tools (version %s, %s)</i></p>
<table border="0">
<tr><td><b>platformn</b></td><td>%s</td></tr>
<tr><td><b>run began</b></td><td>%s</tr>
<tr><td><b>run end</b></td><td>%s</tr>
<tr><td><b>duration</b></td><td>%s</tr>
<tr><td><b>user</b></td><td>%s (%s)</tr>
<tr><td><b>command</b></td><td>%s</tr>
<tr><td><b>working directory</b></td><td>%s</tr>
<tr><td><b>mode</b></td><td>%s</td></tr>
</table>
""" % \
        (basename,
        env["ParaMark Version"], env["ParaMark Date"],
        env["platform"], 
        time.strftime("%a %b %d %Y %H:%M:%S %Z", eval(env["run began"])),
        time.strftime("%a %b %d %Y %H:%M:%S %Z", eval(env["run end"])),
        env["duration"],
        env["user"], env["uid"],
        env["command"],
        env["working directory"],
        env["mode"]))

        # I/O performance
        f.write(
"""\
<p align="left"><font size="3" face="Arial"><b><i>I/O Performance (%s/s)</i></b></font></p>
""" % self.unit)
        labels = []
        data = []
        unit = eval(self.unit)
        for opname,thpt in self.io_select("oper,throughput"):
            labels.append(opname)
            data.append(thpt/unit)
        chart = gchart.BarChart()
        chart.adddataseries(0, data)
        chart.setdata()
        chart.setlabels(labels)
        chart.setseriescolor("rainbow")
        if self.chartdatalabels:
            chart.setdatapointlabels()
        f.write(chart.html() + "\n")

        
        # Metadata
        f.write(
"""\
<p align="left"><font size="3" face="Arial"><b><i>Metadata Performance
(ops/sec)</i></b></font></p>
""")
        labels = []
        data = []
        for opname,thpt in self.meta_select("oper,throughput"):
            labels.append(opname)
            data.append(thpt)
        chart = gchart.BarChart()
        chart.adddataseries(0, data)
        chart.setdata()
        chart.setlabels(labels)
        chart.setseriescolor("rainbow")
        if self.chartdatalabels:
            chart.setdatapointlabels()
        f.write(chart.html() + "\n")
        
        f.flush()
        os.fsync(f.fileno())
        f.write(
"""\
<p><i>took %s seconds to generate this report.</i></p>
</body>
</html>
""" % (timer()-start[1]))
        
        f.close()
        ws("Benchmarking report has been written to %s/report.html.\n"
           "Please use your browser to view it.\n" % self.dbhome)
