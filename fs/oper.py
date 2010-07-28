#############################################################################
# ParaMark: Benchmarking Suite for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>

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

# fs/ops.py
# File Operation Primitives

import os
import stat

from modules.verbose import vbs
from modules.utils import *

__all___ = ["write"]

# Internal Constants
TYPE_META = 1
TYPE_IO = 0

OPS_META = ["mkdir", "creat", "access", "open", "open_close", "stat_exist", 
    "stat_non", "utime", "chmod", "rename", "unlink", "rmdir"]

OPS_IO = ["read", "reread", "write", "rewrite", "fread", "freread",
    "fwrite", "frewrite", "offsetread", "offsetwrite"]

DEFAULT_FSIZE=1024
DEFAULT_BLKSIZE=1024

VERBOSE_DEBUG = 1
VERBOSE_DETAIL = 2

# I/O Primitives

class write():
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_CREAT | os.O_RDWR, mode=stat.S_IRUSR | stat.S_IWUSR,
        fsync=False, dryrun=False):
        self.name = "write"
        self.type = TYPE_IO
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.flags = flags
        self.mode = mode
        self.fsync = fsync
        self.dryrun = dryrun
        
        self.opcnt = 0
        self.elapsed = []

    def exe(self):
        """
        open() -> write()... -> [fsync()] -> close()
        """

        if self.dryrun:
            vbs.verbose("write: os.write(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE_DEBUG)
            return None

        blk = '0' * self.bsize
        cnt = int(self.fsize / self.bsize)
        
        # Since we write in block unit, adjust actually written fsize
        if self.fsize % self.bsize != 0:
            cnt += 1
            self.fsize = self.bsize * cnt
        self.opcnt = cnt

        vbs.verbose("write: os.open(%s, %d, %d)" %
            (self.f, self.flags, self.mode), VERBOSE_DEBUG)
        s = timer()
        fd = os.open(self.f, self.flags, self.mode)
        self.elapsed.append(timer() - s)

        while cnt > 0:
            s = timer()
            res = os.write(fd, blk)
            self.elapsed.append(timer() - s)
            if res != self.bsize:
                vbs.warning("written bytes (%d) != bsize (%d)"
                    % (res, self.bsize))
            cnt -= 1

        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.elapsed.append(timer() - s)
        
        vbs.verbose("write: os.close(%d)" % fd, VERBOSE_DEBUG)
        s = timer()
        os.close(fd)
        self.elapsed.append(timer() - s)

    def get(self):
        out = {}
        out["oper"] = "write"
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["flags"] = self.flags
        out["mode"] = self.mode
        out["fsync"] = self.fsync
        desc = "fd = open(%s, %d, %d) -> write(fd, %d) * %d" \
            % (self.f, self.flags, self.mode, self.bsize, self.opcnt)
        if self.fsync: desc += " -> fsync(fd)"
        desc += " -> close(fd)"
        out["desc"] = desc
        out["elapsed"] = self.elapsed
        return out
