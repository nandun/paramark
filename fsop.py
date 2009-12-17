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
# fsop.py
# File system operation primitives
#

import __builtin__  # for open()
import os
import sys
import stat
import random

from common import *

__all__ = ["FSOP_META", "FSOP_IO"]

class metaop:
    """Metadata operation base class"""
    def __init__(self, name, files, verbose=False, dryrun=False):
        assert files is not None
        self.name = name 
        self.files = files
        self.verbose = verbose
        self.dryrun = dryrun
        self.res = []
    
    def updatekw(self, kw):
        if kw is not None:
            for k, v in kw:
                if self.__dict__.has_key(k): self.__dict__[k] = v

    def vs(self, msg):
        sys.stderr.write("%s\n" % msg)
    
    def execute(self):
        sys.stderr.write("nometaop\n")
        return None

FSOP_META = ["mkdir", "rmdir", "creat", "access", "open","open_close", 
    "stat_exist", "stat_non", "utime", "chmod", "rename", "unlink"]
__all__.extend(FSOP_META)

class mkdir(metaop):
    """Make a list of directories by os.mkdir()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "mkdir", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files: self.vs("os.mkdir(%s)" % f)
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.mkdir(f)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res
        
class rmdir(metaop):
    """Remove a list of directories by os.rmdir()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "rmdir", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files: self.vs("os.rmdir(%s)" % f)
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.rmdir(f)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res

class creat(metaop):
    """Create a list of files by os.open() and os.close() pairs"""
    def __init__(self, files, flags= os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 
        mode=stat.S_IRUSR | stat.S_IWUSR, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "creat", files, verbose, dryrun)
        self.flags = flags
        self.mode = mode
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.close(os.open(%s, 0x%x, 0x%x))" 
                    % (f, self.flags, self.mode))
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.close(os.open(f, self.flags, self.mode))
            self.res.append((s, timer()))

        assert len(self.res) == len(self.files)
        return self.res

class access(metaop):
    """Access a list of files by os.access()"""
    def __init__(self, files, mode=os.F_OK, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "access", files, verbose, dryrun)
        self.mode = mode
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.access(%s, 0x%x)" % (f, self.mode))
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.access(f, self.mode)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res

class open(metaop): # shadows __builtin__.open
    """Open a list of files by os.open()"""
    def __init__(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False,
        **kw):
        metaop.__init__(self, "open", files, verbose, dryrun)
        self.flags = flags
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.open(%s, 0x%x)" % (f, self.flags))
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            fd = os.open(f, self.flags)
            self.res.append((s, timer()))
            os.close(fd)
        
        assert len(self.res) == len(self.files)
        return self.res

class open_close(metaop):
    """Access a list of files by os.open() and os.close() pairs"""
    def __init__(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False,
        **kw):
        metaop.__init__(self, "open_close", files, verbose, dryrun)
        self.flags = flags
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.close(os.open(%s, 0x%x))" % (f, self.flags))
        if self.dryrun: return None
            
        for f in self.files:
            s = timer()
            os.close(os.open(f, self.flags))
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res

class stat_exist(metaop):
    """Access a list of files by os.stat()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "stat_exist", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files: self.vs("os.stat(%s)" % f)
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.stat(f)
            self.res.append((s, timer()))

        assert len(self.res) == len(self.files)
        return self.res 

class stat_non(metaop):
    """Access a list of NON-EXIST files by os.stat()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "stat_non", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        nfiles = map(lambda f:f+'n', self.files)
        if self.verbose:
            for f in nfiles: self.vs("os.stat(%s)" % f)
        if self.dryrun: return None
        
        for f in nfiles:
            s = timer()
            try: os.stat(f)
            except: pass
            self.res.append((s, timer()))

        assert len(self.res) == len(nfiles)
        return self.res

class utime(metaop):
    """Access a list of files by os.utime()"""
    def __init__(self, files, times=None, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "utime", files, verbose, dryrun)
        self.times = times
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.utime(%s, %s)" % (f, self.times))
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.utime(f, self.times)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res 

class chmod(metaop):
    """Access a list of files by os.chmod()"""
    def __init__(self, files, mode=stat.S_IEXEC, verbose=False, dryrun=False,
        **kw):
        metaop.__init__(self, "chmod", files, verbose, dryrun)
        self.mode = mode
        self.updatekw(kw)
   
    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.chmod(%s, 0x%x)" % (f, self.mode))
        if self.dryrun: return None
           
        for f in self.files:
            s = timer()
            os.chmod(f, self.mode)
            self.res.append((s, timer()))

        assert len(self.res) == len(self.files)
        return self.res 

class rename(metaop):
    """Access a list of files by os.rename()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "rename", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            for f in self.files:
                self.vs("os.rename(%s, %s.to)" % (f, f))
        if self.dryrun: return None
        
        for f in self.files:
            tofile = f + ".to"
            s = timer()
            os.rename(f, tofile)
            self.res.append((s, timer()))
        assert len(self.res) == len(self.files)
        
        # rename back
        for f in self.files:
            tofile = f + ".to"
            if self.verbose: self.vs("os.rename(%s, %s) back" % (tofile, f))
            os.rename(tofile, f)

        return self.res

class unlink(metaop):
    """Unlink a list of files by os.unlink()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "unlink", files, verbose, dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            for f in self.files: self.vs("os.unlink(%s)" % f)
        if self.dryrun: return None
        
        for f in self.files:
            s = timer()
            os.unlink(f)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res 

class ioop:
    """I/O operation base class"""
    def __init__(self, name, file, fsize, blksize, flags, verbose=False,
        dryrun=False):
        self.name = name
        self.file = file
        self.fsize = fsize
        self.blksize = blksize
        self.flags = flags
        self.verbose = verbose
        self.dryrun = dryrun
        self.res = []
    
    def updatekw(self, kw):
        if kw is not None:
            for k, v in kw:
                if self.__dict__.has_key(k): self.__dict__[k] = v
    
    def vs(self, msg):
        sys.stderr.write("%s\n" % msg)

    def execute(self):
        sys.stderr.write("noop\n")
        return None

FSOP_IO = ["read", "reread", "write", "rewrite", "fread", "freread",
    "fwrite", "frewrite", "offsetread", "offsetwrite"]
__all__.extend(FSOP_IO)

class read(ioop):
    """Read a file by os.read() with give parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_RDONLY, verbose=False,
        dryrun=False, **kw):
        ioop.__init__(self, "read", file, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("os.read(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        # Be careful!
        # The first record is open time and the last record is close time
        ret = 1
        s = timer()
        fd = os.open(self.file, self.flags)
        self.res.append((s, timer()))
        while ret:
            s = timer()
            ret = os.read(fd, self.blksize)
            self.res.append((s, timer()))
            #assert len(ret) == blksize
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class reread(ioop):
    """Reread a file by os.read() with given parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_RDONLY, verbose=False, 
        dryrun=False, **kw):
        ioop.__init__(self, "reread", file, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            self.vs("os.read(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None

        ret = 1
        s = timer()
        fd = os.open(self.file, self.flags)
        self.res.append((s, timer()))
        while ret:
            s = timer()
            ret = os.read(fd, self.blksize)
            self.res.append((s, timer()))
            #assert len(ret) == blksize
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class write(ioop):
    """write a file by os.write() with given parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR, 
        mode=stat.S_IRUSR | stat.S_IWUSR, byte='0', fsync=False,
        verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "write", file, fsize, blksize, flags, verbose,
        dryrun)
        self.mode = mode
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)
       
    def execute(self):
        if self.verbose:
            self.vs("os.write(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        fd = os.open(self.file, self.flags, self.mode)
        self.res.append((s, timer()))
        while writebytes < self.fsize:
            s = timer()
            ret = os.write(fd, block)
            self.res.append((s, timer()))
            assert ret == self.blksize
            writebytes += ret
        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.res.append((s, timer()))
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class rewrite(ioop):
    """Re-write a file by os.write() with given parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR,
        mode=stat.S_IRUSR | stat.S_IWUSR, byte='1', fsync=False, 
        verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "rewrite", file, fsize, blksize, flags, verbose,
        dryrun)
        self.mode = mode
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            self.vs("os.write(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        self.res = []
        s = timer()
        fd = os.open(self.file, self.flags, self.mode)
        self.res.append((s, timer()))
        while writebytes < self.fsize:
            s = timer()
            ret = os.write(fd, block)
            self.res.append((s, timer()))
            assert ret == self.blksize
            writebytes += ret
        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.res.append((s, timer()))
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class fread(ioop):
    """Read a file by f.read() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='r', verbose=False,
        dryrun=False, **kw):
        ioop.__init__(self, "fread", file, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.read(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        ret = 1
        s = timer()
        f = __builtin__.open(self.file, self.flags)
        self.res.append((s, timer()))
        while ret:
            s = timer()
            ret = f.read(self.blksize)
            self.res.append((s, timer()))
            #assert len(ret) == blksize
        s = timer()
        f.close()
        self.res.append((s, timer()))

        return self.res

class freread(ioop):
    """Read a file by f.read() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='r', verbose=False,
        dryrun=False, **kw):
        ioop.__init__(self, "freread", file, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.read(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        ret = 1
        s = timer()
        f = __builtin__.open(self.file, self.flags)
        self.res.append((s, timer()))
        while ret:
            s = timer()
            ret = f.read(self.blksize)
            self.res.append((s, timer()))
            #assert len(ret) == blksize
        s = timer()
        f.close()
        self.res.append((s, timer()))

        return self.res

class fwrite(ioop):
    """write a file by f.write() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='w', byte='2', fsync=False,
        verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "fwrite", file, fsize, blksize, flags, verbose,
        dryrun)
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.write(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        f = __builtin__.open(self.file, self.flags)
        self.res.append((s, timer()))
        while writebytes < self.fsize:
            s = timer()
            f.write(block)
            self.res.append((s, timer()))
            writebytes += self.blksize
        if self.fsync:
            s = timer()
            f.flush()
            os.fsync(f.fileno())
            self.res.append((s, timer()))
        s = timer()
        f.close()
        self.res.append((s, timer()))

        return self.res

class frewrite(ioop):
    """Re-write a file by f.write() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='w', byte='3', fsync=False,
        verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "frewrite", file, fsize, blksize, flags, verbose,
        dryrun)
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.write(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        f = __builtin__.open(self.file, self.flags)
        self.res.append((s, timer()))
        while writebytes < self.fsize:
            s = timer()
            f.write(block)
            self.res.append((s, timer()))
            writebytes += self.blksize
        if self.fsync:
            s = timer()
            f.flush()
            os.fsync(f.fileno())
            self.res.append((s, timer()))
        s = timer()
        f.close()
        self.res.append((s, timer()))

        return self.res

class offsetread(ioop):
    """Read a file by os.read() with offsets in a certain distribution"""
    def __init__(self, file, fsize, blksize, flags=os.O_RDONLY, 
        dist=None, verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "offsetread", file, fsize, blksize, flags, 
        verbose, dryrun)
        self.dist = dist
        self.updatekw(kw)
    
    def execute(self):
        # Generate distribution
        randwalk = []
        opcnt = self.fsize / self.blksize
        if self.dist is None or self.dist[0] == "random":
            i = 0
            while i < opcnt:
                offset = random.randint(0, self.fsize)
                assert offset > 0 and offset < self.fsize
                randwalk.append(offset)
                i += 1
        elif self.dist[0] == "normal":
            dist_name, dist_mu, dist_sigma = self.dist
            # TODO: Calculate mu and sigma
            # if mu is None: mu = 
            # if sigma is None: sigma = 
            i = 0
            while i < opcnt:
                offset = int(round(random.normalvariate(dist_mu, dist_sigma)))
                assert offset > 0 and offset < self.fsize
                randwalk.append(offset)
                i += 1
        
        if self.verbose:
            for offset in randwalk:
                self.vs("os.read(%s, %d) * %d at %d" %
                    (self.file, self.blksize, self.fsize/self.blksize, offset))
        if self.dryrun: return None
        
        s = timer()
        fd = os.open(self.file, self.flags)
        self.res.append((s, timer()))
        for offset in randwalk:
            s = timer()
            os.lseek(fd, offset, os.SEEK_SET)
            os.read(fd, self.blksize)
            self.res.append((s, timer()))
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class offsetwrite(ioop):
    """Write a file by os.write() with offsets in a certain distribution"""
    def __init__(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR, 
        byte='4', dist=None, fsync=False, verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "offsetwrite", file, fsize, blksize, flags, 
        verbose, dryrun)
        self.byte = byte
        self.dist = dist
        self.fsync = fsync
        self.updatekw(kw)

    def execute(self):
        # Generate distribution
        randwalk = []
        opcnt = self.fsize / self.blksize
        if self.dist is None or self.dist[0] == "random":
            i = 0
            while i < opcnt:
                offset = random.randint(0, self.fsize)
                assert offset > 0 and offset < self.fsize
                randwalk.append(offset)
                i += 1
        elif self.dist[0] == "normal":
            dist_name, dist_mu, dist_sigma = self.dist
            # TODO: Calculate mu and sigma
            # if mu is None: mu = 
            # if sigma is None: sigma = 
            i = 0
            while i < opcnt:
                offset = int(round(random.normalvariate(dist_mu, dist_sigma)))
                assert offset > 0 and offset < self.fsize
                randwalk.append(offset)
                i += 1
        
        if self.verbose:
            for offset in randwalk:
                self.vs("os.write(%s, %d) * %d at %d" %
                    (self.file, self.blksize, self.fsize/self.blksize, offset))
        if self.dryrun: return None

        block = self.byte * self.blksize
        s = timer()
        fd = os.open(self.file, self.flags)
        self.res.append((s, timer()))
        for offset in randwalk:
            s = timer()
            os.lseek(fd, offset, os.SEEK_SET)
            os.write(fd, block)
            self.res.append((s, timer()))
        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.res.append((s, timer()))
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res
