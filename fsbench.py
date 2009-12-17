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
import pwd
import socket
import copy

from common import *
import fsop

class Bench():
    """General file system benchmark"""
    def __init__(self, opts=None, **kw):

        # Benchmark-time environment variables
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        self.cmdline = " ".join(sys.argv)
        self.environ = copy.deepcopy(os.environ)

    def vs(self, msg):
        sys.stderr.write(msg)

if __name__ == "__main__":
    b = Bench()
    print b.environ
