#!/usr/bin/env python

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
# fsbench.py
# General File System Benchmark
#

import sys
import os
import errno
import pwd
import socket
import time
import copy

import version
import utils
import fs.opts
import fs.bench
import fs.data
import fs.report

class Bench():
    """General file system benchmark"""
    def __init__(self, argv, **kw):
        self.config = fs.opts.Options()
        self.opts, errstr = self.config.load(argv)
        if errstr:
            sys.stderr("error: %s\n" % errstr)
            sys.exit(1)

        # Post check and preparation
        self.opts["wdir"] = os.path.abspath(self.opts["wdir"])
        if self.opts["logdir"] is None:  # generate random logdir in cwd
            self.opts["logdir"] = os.path.abspath("./pmlog-%s-%s" %
                (self.user, time.strftime("%j-%H-%M-%S")))

        # Benchmark runtime environment
        self.runtime = {}
        self.runtime["version"] = version.PARAMARK_VERSION
        self.runtime["date"] = version.PARAMARK_DATE
        self.runtime["uid"] = os.getuid()
        self.runtime["pid"] = os.getpid()
        self.runtime["user"] = pwd.getpwuid(os.getuid())[0]
        self.runtime["hostname"] = socket.gethostname()
        self.runtime["platform"] = " ".join(os.uname())
        self.runtime["cmdline"] = " ".join(sys.argv)
        self.runtime["environ"] = copy.deepcopy(os.environ)
        self.runtime["mountpoint"] = utils.get_mountpoint(self.opts["wdir"])
        self.runtime["wdir"] = self.opts["wdir"]

        # runtime passing variables
        self.threads = []  # run set

    def vs(self, msg):
        sys.stderr.write(msg)
         
    def load(self):
        self.opts["hostid"] = 0
        self.opts["pid"] = os.getpid()
        self.opts["verbose"] = False
        if self.opts["verbosity"] >= 3:
            self.opts["verbose"] = True
        for i in range(0, self.opts["nthread"]):
            self.opts["tid"] = i
            self.threads.append(fs.bench.BenchThread(self.opts))

    def run(self):
        self.start = utils.timer()
        
        for t in self.threads:
            t.start()

        for t in self.threads:
            t.join()
            for o in t.opset:
                print o.res
        
        if self.opts["dryrun"]:
            sys.stdout.write("Dryrun, nothing executed.\n")
        
        self.end = utils.timer()
        
        self.runtime["start"] = "%r" % self.start
        self.runtime["end"] = "%r" % self.end

    def save(self):
        if self.opts["dryrun"]: return
        
        # Intial log directory and database
        self.opts["logdir"] = utils.smart_makedirs(self.opts["logdir"],
            self.opts["confirm"])
        logdir = os.path.abspath(self.opts["logdir"])
        
        # Save used configuration file
        self.config.save_conf("%s/fsbench.conf" % logdir)
        if self.opts["verbosity"] >= 1:
            self.vs("applied configurations saved to %s/fsbench.conf\n" 
                % logdir)
        
        # Save results
        self.db = fs.data.Database("%s/fsbench.db" % logdir, True)
        self.db.runtime_ins(self.runtime)
        self.db.data_ins(self.rset, self.start)
        self.db.close()

        if self.opts["verbosity"] >= 1:
            self.vs("raw benchmark data saved to %s/fsbench.db\n" % logdir)

#
# Main entry
#

def standalone_main(argv):
    # Get options and configurations
    if len(argv) > 2 and argv[1] == "report":
        if len(argv) < 3:
            sys.stderr.write("usage: %s report DATA_DIR\n" % argv[0])
            sys.exit(1)
        myreport = fs.report.HTMLReport(argv[2])
        myreport.produce()
    else:
        mybench = Bench(argv)
        mybench.load()
        mybench.run()
        #mybench.save()
        #myreport = fsreport.HTMLReport(mybench.opts["logdir"])
        #myreport.produce()

if __name__ == "__main__":
    sys.exit(standalone_main(sys.argv))

# EOF
