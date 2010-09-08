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
from __builtin__ import open as _open

from modules.verbose import *
from modules.common import *

VERBOSE = 1
VERBOSE_MORE = VERBOSE + 1

TYPE_META = 1
TYPE_IO = 0

OPS_META = ["mkdir", "creat", "access", "open", "open_close", "stat_exist", 
    "stat_non", "utime", "chmod", "rename", "unlink", "rmdir"]

OPS_IO = ["write", "rewrite", "read", "reread", "fwrite", "frewrite", 
    "fread", "freread"]

DEFAULT_FSIZE = 1024
DEFAULT_BLKSIZE = 1024

DEFAULT_OPCNT = 100
DEFAULT_FACTOR = 16


# Utilities
def optype(opname):
    if opname in OPS_META: return TYPE_META
    elif opname in OPS_IO: return TYPE_IO

# I/O Primitives
class read:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_RDONLY, mode=stat.S_IRUSR, dryrun=False):
        self.name = "read"
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.flags = flags
        self.mode = mode
        self.dryrun = dryrun
        self.opcnt = 0
        self.elapsed = []
        self.synctime = None

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
        out["synctime"] = self.synctime
        return out

class reread:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE, 
        flags=os.O_RDONLY, mode=stat.S_IRUSR, dryrun=False):
        self.name = "reread"
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.flags = flags
        self.dryrun = dryrun
        self.opcnt = 0
        self.elapsed = []
        self.synctime = None

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
        out["synctime"] = self.synctime
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
        self.synctime = None

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
        out["name"] = self.name
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["flags"] = self.flags
        out["mode"] = self.mode
        out["fsync"] = self.fsync
        out["elapsed"] = self.elapsed
        out['synctime'] = self.synctime
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
        self.synctime = None

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
        out["synctime"] = self.synctime
        return out

class fread:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE,
        mode='r', bufsize=-1, dryrun=False):
        self.name = 'fread'
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.mode = mode
        self.bufsize = bufsize
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        if self.dryrun:
            verbose(" fread: file.read(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        cnt = int(self.fsize / self.bsize)
        if self.fsize % self.bsize != 0: cnt += 1
        self.opcnt = cnt

        verbose(" fread: open(%s, %s, %d)" %
            (self.f, self.mode, self.bufsize), VERBOSE_MORE)
        s = timer()
        f = _open(self.f, self.mode, self.bufsize)
        self.elapsed.append(timer() - s)

        verbose(" fread: f.read(%s) * %d" %
            (self.bsize, self.fsize / self.bsize), VERBOSE)
        while cnt > 0:
            s = timer()
            res = f.read(self.bsize)
            self.elapsed.append(timer() - s)
            if len(res) != self.bsize:
                warning("fread bytes (%d) != bsize (%d)"
                    % (len(res), self.bsize))
            cnt -= 1

        verbose(" fread: f.close()", VERBOSE_MORE)
        s = timer()
        f.close()
        self.elapsed.append(timer() - s)
    
    def get(self):
        out = {}
        out["name"] = self.name
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["mode"] = self.mode
        out["elapsed"] = self.elapsed
        out["synctime"] = self.synctime
        return out

class freread:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE,
        mode='r', bufsize=-1, dryrun=False):
        self.name = 'freread'
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.mode = mode
        self.bufsize = bufsize
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        if self.dryrun:
            verbose(" freread: file.read(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        cnt = int(self.fsize / self.bsize)
        if self.fsize % self.bsize != 0: cnt += 1
        self.opcnt = cnt
        
        verbose(" freread: open(%s, %s, %d)" %
            (self.f, self.mode, self.bufsize), VERBOSE_MORE)
        s = timer()
        f = _open(self.f, self.mode, self.bufsize)
        self.elapsed.append(timer() - s)

        verbose(" freread: f.read(%d) * %d" %
            (self.bsize, self.fsize / self.bsize), VERBOSE)
        while cnt > 0:
            s = timer()
            res = f.read(self.bsize)
            self.elapsed.append(timer() - s)
            if len(res) != self.bsize:
                warning("freread bytes (%d) != bsize (%d)"
                    % (len(res), self.bsize))
            cnt -= 1
        
        verbose(" freread: f.close()", VERBOSE_MORE)
        s = timer()
        f.close()
        self.elapsed.append(timer() - s)
    
    def get(self):
        out = {}
        out["name"] = self.name
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["mode"] = self.mode
        out["elapsed"] = self.elapsed
        out["synctime"] = self.synctime
        return out

class fwrite:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE,
        mode='w', bufsize=-1, fsync=False, dryrun=False):
        self.name = 'fwrite'
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.mode = mode
        self.bufsize = bufsize
        self.fsync = fsync
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        if self.dryrun:
            verbose(" fwrite: file.write(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        blk = '2' * self.bsize
        cnt = int(self.fsize / self.bsize)
        # Since we write in block unit, adjust actually written fsize
        if self.fsize % self.bsize != 0:
            cnt += 1
            self.fsize = self.bsize * cnt
        self.opcnt = cnt
        
        verbose(" fwrite: open(%s, %s, %d)" %
            (self.f, self.mode, self.bufsize), VERBOSE_MORE)
        s = timer()
        f = _open(self.f, self.mode, self.bufsize)
        self.elapsed.append(timer() - s)

        while cnt > 0:
            s = timer()
            f.write(blk)
            self.elapsed.append(timer() - s)
            cnt -= 1

        if self.fsync:
            verbose(" fwrite: f.flush(); os.fsync(%d)" % f.fileno())
            s = timer()
            f.flush()
            os.fsync(f.fileno())
            self.elapsed.append(timer() - s)

        verbose(" fwrite: f.close()", VERBOSE_MORE)
        s = timer()
        f.close()
        self.elapsed.append(timer() - s)
    
    def get(self):
        out = {}
        out["name"] = self.name
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["mode"] = self.mode
        out["fsync"] = self.fsync
        out["elapsed"] = self.elapsed
        out["synctime"] = self.synctime
        return out

class frewrite:
    def __init__(self, f, fsize=DEFAULT_FSIZE, bsize=DEFAULT_BLKSIZE,
        mode='w', bufsize=-1, fsync=False, dryrun=False):
        self.name = 'frewrite'
        self.f = f
        self.fsize = fsize
        self.bsize = bsize
        self.mode = mode
        self.bufsize = bufsize
        self.fsync = fsync
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        if self.dryrun:
            verbose(" frewrite: file.write(%s, %d) * %d" %
                (self.f, self.bsize, self.fsize/self.bsize), VERBOSE)
            return

        blk = '3' * self.bsize
        cnt = int(self.fsize / self.bsize)
        # Since we write in block unit, adjust actually written fsize
        if self.fsize % self.bsize != 0:
            cnt += 1
            self.fsize = self.bsize * cnt
        self.opcnt = cnt
        
        verbose(" frewrite: open(%s, %s, %d)" %
            (self.f, self.mode, self.bufsize), VERBOSE_MORE)
        s = timer()
        f = _open(self.f, self.mode, self.bufsize)
        self.elapsed.append(timer() - s)

        while cnt > 0:
            s = timer()
            f.write(blk)
            self.elapsed.append(timer() - s)
            cnt -= 1

        if self.fsync:
            verbose(" frewrite: f.flush(); os.fsync(%d)" % f.fileno())
            s = timer()
            f.flush()
            os.fsync(f.fileno())
            self.elapsed.append(timer() - s)

        verbose(" frewrite: f.close()", VERBOSE_MORE)
        s = timer()
        f.close()
        self.elapsed.append(timer() - s)
    
    def get(self):
        out = {}
        out["name"] = self.name
        out["file"] = self.f
        out["fsize"] = self.fsize
        out["bsize"] = self.bsize
        out["mode"] = self.mode
        out["fsync"] = self.fsync
        out["elapsed"] = self.elapsed
        out["synctime"] = self.synctime
        return out
        
# Metadata Primitives
class mkdir:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR, 
        dryrun=False):
        self.name = 'mkdir'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" mkdir: os.mkdir(%d directories)" % self.opcnt, VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            os.mkdir(f)
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class rmdir:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, 
        factor=DEFAULT_FACTOR, dryrun=False):
        self.name = 'rmdir'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" rmdir: os.rmdir(%d directories)" % self.opcnt, VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            os.rmdir(f)
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class creat:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR,
        flags=os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 
        mode=stat.S_IRUSR | stat.S_IWUSR, dryrun=False):
        self.name = 'creat'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.flags = flags
        self.mode = mode
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        self.elapsed = []
        self.synctime = None
    
    def exe(self):
        verbose(" creat: os.close(os.open(%d files))" % 
            len(self.files), VERBOSE)
        if self.dryrun: return

        for f in self.files:
            s = timer()
            os.close(os.open(f, self.flags, self.mode))
            self.elapsed.append(timer() - s)

        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)
    
    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class access:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR, 
        mode=os.F_OK, dryrun=False):
        self.name = 'access'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.mode = mode
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" access: os.access(%d files)" % self.opcnt, VERBOSE)
        for f in self.files:
            s = timer()
            os.access(f, self.mode)
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)
    
    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class open: # shadows __builtin__.open
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR, 
        flags=os.O_RDONLY, mode=stat.S_IRUSR, dryrun=False):
        self.name = 'open'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.flags = flags
        self.mode = mode
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" open: os.open(%d files)" % self.opcnt, VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            fd = os.open(f, self.flags, self.mode)
            self.elapsed.append(timer() - s)
            os.close(fd)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class open_close: # shadows __builtin__.open
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR, 
        flags=os.O_RDONLY, mode=stat.S_IRUSR, dryrun=False):
        self.name = 'open_close'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.flags = flags
        self.mode = mode
        self.dryrun = dryrun
        assert len(self.files) == self.opcnt
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" open_close: os.close(os.open(%d files))" % 
            self.opcnt, VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            os.close(os.open(f, self.flags, self.mode))
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class stat_exist:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, 
        factor=DEFAULT_FACTOR, dryrun=False):
        self.name = 'stat_exist'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" stat_exist: os.stat(%d existing files)" % self.opcnt,
            VERBOSE)
        if self.dryrun: return

        for f in self.files:
            s = timer()
            os.stat(f)
            self.elapsed.append(timer() - s)

        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class stat_non:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, 
        factor=DEFAULT_FACTOR, dryrun=False):
        self.name = 'stat_non'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" stat_non: os.stat(%d non-existing files)" % self.opcnt,
            VERBOSE)
        if self.dryrun: return

        for f in map(lambda f:f+'.non', self.files):
            s = timer()
            try: os.stat(f)
            except OSError: pass
            self.elapsed.append(timer() - s)

        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class utime:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, 
        factor=DEFAULT_FACTOR, times=None, dryrun=False):
        self.name = 'utime'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.times = times
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" utime: os.utime(%d files, %s)" % 
            (self.opcnt, self.times), VERBOSE)
        if self.dryrun: return

        for f in self.files:
            s = timer()
            os.utime(f, self.times)
            self.elapsed.append(timer() - s)

        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class chmod:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, 
        factor=DEFAULT_FACTOR, mode=stat.S_IEXEC, dryrun=False):
        self.name = 'chmod'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.mode = mode
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None
   
    def exe(self):
        verbose(" chmod: os.chmod(%d files, 0x%x)" % 
            (self.opcnt, self.mode), VERBOSE)
        if self.dryrun: return
        
        for f in self.files:
            s = timer()
            os.chmod(f, self.mode)
            self.elapsed.append(timer() - s)

        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class rename:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR, 
        dryrun=False):
        self.name = 'rename'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" rename: os.rename(%d files)" % self.opcnt, VERBOSE)
        if self.dryrun: return

        fromtos = map(lambda f:(f, f+".to"), self.files)

        for f, t in fromtos:
            s = timer()
            os.rename(f, t)
            self.elapsed.append(timer() - s)

        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)
            
            # rename back
            for f, t in fromtos: os.rename(t, f)
    
    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out

class unlink:
    def __init__(self, files, opcnt=DEFAULT_OPCNT, factor=DEFAULT_FACTOR, 
        dryrun=False):
        self.name = 'unlink'
        self.files = files
        self.opcnt = opcnt
        self.factor = factor
        self.dryrun = dryrun
        self.elapsed = []
        self.synctime = None

    def exe(self):
        verbose(" unlink: os.unlink(%d files)" % self.opcnt, VERBOSE)
        if self.dryrun: return

        for f in self.files:
            s = timer()
            os.unlink(f)
            self.elapsed.append(timer() - s)
        
        if not self.dryrun:
            assert self.opcnt == len(self.elapsed)

    def get(self):
        out = {}
        out['name'] = self.name
        out['opcnt'] = self.opcnt
        out['factor'] = self.factor
        out['elapsed'] = self.elapsed
        out['synctime'] = self.synctime
        return out
