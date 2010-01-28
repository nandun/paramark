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
# fs/load.py
# File System Load Generator
#

import os
import socket
import random
import copy

from oper import FSOP_META, FSOP_IO

class MetaLoad:
    """Metadata workload generator"""
    def __init__(self, opts, **kw):
        for o in ["wdir"]: self.__dict__[o] = opts[o]
        # only retrieve metadata part options
        self.opts = {}
        for o in FSOP_META + FSOP_IO: self.opts[o] = opts[o]
        
        for k, v in opts["metaopts"].items(): 
            self.__dict__[k] = v

        self.rdir = None    # root directory to perform load
        self.dirs = None
        self.dir = None
        self.files = None
        self.file = None

    def produce(self):
        self.rdir = "%s/pmark-wdir-%s-%s-%03d" \
            % (self.wdir, socket.gethostname(), os.getpid(), \
               random.randint(0,999))
        
        self.dirs = []
        queue = [ copy.deepcopy(self.rdir) ]
        i = l = 0
        while i < self.opcnt:
            if i % self.factor == 0:
                parent = queue.pop(0)
                l += 1
            child = os.path.normpath("%s/L%d-%d" % (parent, l, i))
            self.dirs.append(child)
            queue.append(child)
            i += 1

    def getrootdir(self):
        return self.rdir

    def mkdir(self):
        # TODO: setup on local options
        return self.dirs

    def rmdir(self):
        # TODO: setup on local options
        dirs = list(self.dirs)
        dirs.reverse()
        return dirs

class IOLoad:
    """I/O workload generator"""
    def __init__(self, opts, **kw):
        self.opts = opts

# EOF
