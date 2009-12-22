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

import common
import fsopts
import fsop
import fsload

class Bench():
    """General file system benchmark"""
    def __init__(self, opts=None, **kw):
        self.config = fsopts.Options()

        # Benchmark-time environment variables
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        self.cmdline = " ".join(sys.argv)
        self.environ = copy.deepcopy(os.environ)
        
        # runtime passing variables
        self.rset = []  # run set

    def vs(self, msg):
        sys.stderr.write(msg)
         
    def load(self, argv):
        self.opts, errstr = self.config.load(argv)
        if errstr:
            sys.stderr("error: %s\n" % errstr)
            return 1

        # Post check and preparation
        if self.opts["logdir"] is None:  # generate random logdir in cwd
            self.opts["logdir"] = os.path.abspath("./pmlog-%s-%s" %
                (self.user, time.strftime("%j-%H-%M-%S")))
        
        self.metaload = fsload.MetaLoad(self.opts)
        self.ioload = fsload.IOLoad(self.opts)
        self.metaload.produce()

        # Generate run set
        #  1. Set up target file for each operation
        #for o in self.opts["metaops"] + self.opts["ioops"]:
        for o in ["mkdir", "rmdir"]:
            if self.opts["verbosity"] >= 3: verbose = True
            else: verbose = False
            ostr = "fsop.%s(files=self.metaload.%s(), " \
                "verbose=%s, dryrun=%s, **self.opts[\"%s\"])" \
                % (o, o, verbose, self.opts["dryrun"], o)
            self.rset.append(eval(ostr))

    def run(self):
        self.start = common.timer()
        if not self.opts["dryrun"]:
            os.makedirs(self.metaload.getrootdir())

        for r in self.rset: r.execute()

        if not self.opts["dryrun"]:
            import shutil
            shutil.rmtree(self.metaload.getrootdir())

        if self.opts["dryrun"]:
            sys.stdout.write("Dryrun, nothing executed.\n")
        
        self.end = common.timer()

    def save(self):
        if self.opts["dryrun"]: return
        
        # Intial log directory and database
        self.opts["logdir"] = common.smart_makedirs(self.opts["logdir"],
            self.opts["confirm"])
        logdir = os.path.abspath(self.opts["logdir"])
        
        # Save used configuration file
        self.config.save_conf("%s/fsbench.conf" % logdir)
        if self.opts["verbosity"] >= 1:
            self.vs("applied configurations saved to %s/fsbench.conf\n" % logdir)
        
        # Save results
        import fsdata
        self.db = fsdata.Database("%s/fsbench.db" % logdir, True)

        self.save_runtime()
        for r in self.rset:
            r.res = map(lambda (s,e):(s-self.start,e-self.start), r.res)
            if self.opts["verbosity"] >= 4:
                self.vs("saving %s res=%s\n" % (r.name, r.res))
            self.db.rawdata_ins(r.name, self.db.obj2str(r.res))
        self.db.close()
        if self.opts["verbosity"] >= 1:
            self.vs("raw benchmark data saved to %s/fsbench.db\n" % logdir)

        sys.stdout.write("Done! See %s for reports\n" % logdir)

    def save_runtime(self):
        from version import PARAMARK_VERSION, PARAMARK_DATE
        self.db.runtime_ins("version", PARAMARK_VERSION)
        self.db.runtime_ins("date", PARAMARK_DATE)
        self.db.runtime_ins("platform", self.platform)
        self.db.runtime_ins("start", "%r" % self.start)
        self.db.runtime_ins("end", "%r" % self.end)
        self.db.runtime_ins("user", self.user)
        self.db.runtime_ins("uid", "%s" % self.uid)
        self.db.runtime_ins("pid", "%s" % self.pid)
        self.db.runtime_ins("cmdline", self.cmdline)
        #TODO: save environ as string
        self.db.runtime_ins("environ", self.db.obj2str(self.environ))
        self.db.commit()
# EOF
