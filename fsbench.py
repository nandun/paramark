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
import options
import fsop

class Bench():
    """General file system benchmark"""
    def __init__(self, opts=None, **kw):
        self.config = options.Options()

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
            self.opts["logdir"] = os.path.abspath("./pmark-%s-%s" %
                (self.user, time.strftime("%j-%H-%M-%S")))
    
    def save(self):
        self.opts["logdir"] = common.smart_makedirs(self.opts["logdir"],
            self.opts["confirm"])
        logdir = os.path.abspath(self.opts["logdir"])
        print logdir
        
        # Save used configuration file
        if self.opts["verbosity"] >= 3:
            self.vs("Saving applied configurations to %s/config ...\n" 
                % logdir)
        self.config.save_conf("%s/config" % logdir)

    def prepare(self):
        """Setup benchmarking parameters and tests"""
        return
