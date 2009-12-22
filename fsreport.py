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

import common
import fsdata

class Report():
    def __init__(self, datadir):
        if not os.path.exists(datadir):
            sys.stderr.write("data directory %s does not exist\n" % datadir)
            sys.exit(1)

        self.datadir = os.path.abspath(datadir)
        self.db = fsdata.Database("%s/fsbench.db" % self.datadir, False)
        
        self.rdir = os.path.abspath("%s/report" % self.datadir)
        if not os.path.exists(self.rdir):
            common.smart_makedirs(self.rdir)
        
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

    def __del__(self):
        self.db.close()
    
    def produce(self):
        eval("self.%s()" % self.opts["format"])

    def html(self):
        import HTMLgen
        HTMLgen.PRINTECHO = 0 # turn off HTMLgen verbose
        
        start = (time.localtime(), common.timer())
        
        SECTION = 2
        SUBSECTION = 3
        # heanding
        doc = HTMLgen.SimpleDocument(
            title="ParaMark Filesytem Benchmark Report")
        heading = HTMLgen.Heading(SECTION, 
            "ParaMark Filesytem Benchmark Report")
        doc.append(heading)

        heading = HTMLgen.Heading(SUBSECTION, "Runtime")
        doc.append(heading)
        
        runtime = {}
        for i, v in self.db.runtime_sel(): runtime[i] = v
        text = HTMLgen.Text("ParaMark v%s, %s" 
            % (runtime["version"], runtime["date"]))
        text.append(HTMLgen.BR())
        text.append("%s" % (runtime["platform"]))
        text.append(HTMLgen.BR())
        text.append("%s (%s)" % (runtime["user"], runtime["uid"]))
        text.append(HTMLgen.BR())
        text.append("%s (%s)" % (runtime["cmdline"], runtime["pid"]))
        # TODO: environ?
        text.append(HTMLgen.BR())
        text.append("%s -- %s (%.5f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"]))))
        doc.append(text)
        
        heading = HTMLgen.Heading(SUBSECTION, "Configuration and Data")
        doc.append(heading)
        
        text = HTMLgen.Text("Configuration ")
        text.append(HTMLgen.Href("../fsbench.conf", "fsbench.conf"))
        text.append(" was applied and data is stored in ")
        text.append(HTMLgen.Href("../fsbench.db", "fsbench.db"))
        text.append(" using ")
        text.append(HTMLgen.Href("http://www.sqlite.org", "SQLite3 format"))
        text.append(".")
        doc.append(text)

        # Generate Results
        
        end = (time.localtime(), common.timer())
        
        # footnote
        text = HTMLgen.Small()
        text.append(HTMLgen.Emphasis(
            "Generated at %s, took %.5f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", end[0]),
              (end[1] - start[1]))))
        
        text.append(HTMLgen.Emphasis(HTMLgen.Href("../report.conf", 
            "report.conf")))
        text.append(" by ")
        from version import PARAMARK_VERSION, PARAMARK_DATE, PARAMARK_WEB
        text.append(HTMLgen.Emphasis(HTMLgen.Href(PARAMARK_WEB, "ParaMark")))
        text.append(HTMLgen.Emphasis(" v%s, %s.\n" 
            % (PARAMARK_VERSION, PARAMARK_DATE)))
        para = HTMLgen.Paragraph(text)
        doc.append(para)
        
        # output
        doc.write("%s/report.html" % self.rdir)
        if not os.path.exists("%s/index.html" % self.rdir):
            os.symlink("%s/report.html" % self.rdir,
                "%s/index.html" % self.rdir)
        sys.stdout.write("Report generated to %s.\n" % self.rdir)
                
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
"""
