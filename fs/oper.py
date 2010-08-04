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
from __builtin__ import open as _open # for open()

from modules.verbose import *
from modules.common import *

__all___ = ["write"]

# Internal Constants
TYPE_META = 1
TYPE_IO = 0

OPS_META = ["mkdir", "creat", "access", "open", "open_close", "stat_exist", 
    "stat_non", "utime", "chmod", "rename", "unlink", "rmdir"]

OPS_IO = ["read", "reread", "write", "rewrite", "fread", "freread",
    "fwrite", "frewrite", "offsetread", "offsetwrite"]

DEFAULT_FSIZE = 1024
DEFAULT_BLKSIZE = 1024

VERBOSE = 1
VERBOSE_MORE = VERBOSE + 1

# Utilities
def optype(opname):
    if opname in OPS_META: return TYPE_META
    elif opname in OPS_IO: return TYPE_IO

# I/O Primitives
class read:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_RDONLY, dryrun=False):
        self.name = "read"
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.flags = flags
        self.dryrun = dryrun
        
        self.opcnt = 0
        self.elapsed = []

    def exe(self):
        if self.dryrun:
            verbose(" read: os.read(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        cnt = int(self.fsize / self.bsize)
        
        if self.fsize % self.bsize != 0: cnt += 1
        self.opcnt = cnt

        verbose(" read: os.open(%s, %d)" % (self.f, self.flags), VERBOSE_MORE)
        s = timer()
        fd = os.open(self.f, self.flags)
        self.elapsed.append(timer() - s)

        verbose(" read: os.read(%s, %d) * %d" %
            (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
        while cnt > 0:
            s = timer()
            res = os.read(fd, self.bsize)
            self.elapsed.append(timer() - s)
            if len(res) != self.bsize:
                warning("read bytes (%d) != bsize (%d)"
                    % (len(res), self.bsize))
            cnt -= 1
        
        verbose(" read: os.close(%d)" % fd, VERBOSE_MORE)
        s = timer()
        os.close(fd)
        self.elapsed.append(timer() - s)

    def get(self):
        out = {}
        out["name"] = "read"
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["flags"] = self.flags
        out["elapsed"] = self.elapsed
        return out

class reread:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_RDONLY, dryrun=False):
        self.name = "reread"
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.flags = flags
        self.dryrun = dryrun
        
        self.opcnt = 0
        self.elapsed = []

    def exe(self):
        if self.dryrun:
            verbose(" reread: os.read(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        cnt = int(self.fsize / self.bsize)
        
        if self.fsize % self.bsize != 0: cnt += 1
        self.opcnt = cnt

        verbose(" reread: os.open(%s, %d)" % 
            (self.f, self.flags), VERBOSE_MORE)
        s = timer()
        fd = os.open(self.f, self.flags)
        self.elapsed.append(timer() - s)

        verbose(" reread: os.read(%s, %d) * %d" %
            (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
        while cnt > 0:
            s = timer()
            res = os.read(fd, self.bsize)
            self.elapsed.append(timer() - s)
            if len(res) != self.bsize:
                warning("read bytes (%d) != bsize (%d)"
                    % (len(res), self.bsize))
            cnt -= 1
        
        verbose(" reread: os.close(%d)" % fd, VERBOSE_MORE)
        s = timer()
        os.close(fd)
        self.elapsed.append(timer() - s)

    def get(self):
        out = {}
        out["name"] = "reread"
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["flags"] = self.flags
        out["elapsed"] = self.elapsed
        return out

class write:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_CREAT | os.O_RDWR, mode=stat.S_IRUSR | stat.S_IWUSR,
        fsync=False, dryrun=False):
        self.name = "write"
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
        if self.dryrun:
            verbose(" write: os.write(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        blk = '0' * self.bsize
        cnt = int(self.fsize / self.bsize)
        
        # Since we write in block unit, adjust actually written fsize
        if self.fsize % self.bsize != 0:
            cnt += 1
            self.fsize = self.bsize * cnt
        self.opcnt = cnt

        verbose(" write: os.open(%s, %d, %d)" %
            (self.f, self.flags, self.mode), VERBOSE_MORE)
        s = timer()
        fd = os.open(self.f, self.flags, self.mode)
        self.elapsed.append(timer() - s)

        verbose(" write: os.write(%s, %d) * %d" %
            (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
        while cnt > 0:
            s = timer()
            res = os.write(fd, blk)
            self.elapsed.append(timer() - s)
            if res != self.bsize:
                warning("written bytes (%d) != bsize (%d)"
                    % (res, self.bsize))
            cnt -= 1

        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.elapsed.append(timer() - s)
        
        verbose(" write: os.close(%d)" % fd, VERBOSE_MORE)
        s = timer()
        os.close(fd)
        self.elapsed.append(timer() - s)

    def get(self):
        out = {}
        out["name"] = "write"
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["flags"] = self.flags
        out["mode"] = self.mode
        out["fsync"] = self.fsync
        out["elapsed"] = self.elapsed
        return out

class rewrite:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_RDWR, mode=stat.S_IRUSR | stat.S_IWUSR,
        fsync=False, dryrun=False):
        self.name = "rewrite"
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
        if self.dryrun:
            verbose(" rewrite: os.write(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return


        blk = '1' * self.bsize
        cnt = int(self.fsize / self.bsize)
        
        # Since we write in block unit, adjust actually written fsize
        if self.fsize % self.bsize != 0:
            cnt += 1
            self.fsize = self.bsize * cnt
        self.opcnt = cnt

        verbose(" rewrite: os.open(%s, %d)" % 
            (self.f, self.flags), VERBOSE_MORE)
        s = timer()
        fd = os.open(self.f, self.flags, self.mode)
        self.elapsed.append(timer() - s)

        verbose(" rewrite: os.open(%s, %d, %d)" %
            (self.f, self.flags, self.mode), VERBOSE)
        while cnt > 0:
            s = timer()
            res = os.write(fd, blk)
            self.elapsed.append(timer() - s)
            if res != self.bsize:
                warning("written bytes (%d) != bsize (%d)"
                    % (res, self.bsize))
            cnt -= 1

        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.elapsed.append(timer() - s)
        
        verbose(" rewrite: os.close(%d)" % fd, VERBOSE_MORE)
        s = timer()
        os.close(fd)
        self.elapsed.append(timer() - s)

    def get(self):
        out = {}
        out["name"] = "rewrite"
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["flags"] = self.flags
        out["mode"] = self.mode
        out["fsync"] = self.fsync
        out["elapsed"] = self.elapsed
        return out

# Metadata Primitives
class mkdir:
    def __init__(self, files, opcnt=100, factor=16, dryrun=False):
        self.name = 'mkdir'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        
        self.elapsed = []

    def exe(self):
        verbose(" mkdir: os.mkdir(%d directories)" % len(self.files), VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            os.mkdir(f)
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = 'mkdir'
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        return out

class rmdir:
    def __init__(self, files, opcnt=100, factor=16, dryrun=False):
        self.name = 'rmdir'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        
        self.elapsed = []

    def exe(self):
        verbose(" rmdir: os.rmdir(%d directories)" % len(self.files), VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            os.rmdir(f)
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = 'rmdir'
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        return out
