#############################################################################
# ParaMark:  A Parallel/Distributed File Systems Benchmark
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
# File system operation primitives
#

# Implementation remarks
#  * Interfaces of operation primitives should be ketp as small as possbile

import os
import sys
import stat
import random

from common import *

class metaop:
    """Metadata operation base class"""
    def __init__(self, name, files, verbose=False, dryrun=False):
        assert files is not None
        self.name = name 
        self.files = files
        self.verbose = verbose
        self.dryrun = dryrun
        self.res = []

    def vs(self, msg):
        sys.stdout.write("%s\n" % msg)

    def exec(self):
        sys.stdout.write("nometaop\n")
        return None

class mkdir(metaop):
    """make a list of directories by os.mkdir()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "mkdir", files, verbose, dryrun)

    def exec(self):
        if self.verbose:
            for file in self.files: self.vs("os.mkdir(%s)" % dir)
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            os.mkdir(file)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return res
        
class rmdir(metaop):
    """remove a list of directories by os.rmdir()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "rmdir", files, verbose, dryrun)

    def exec(self):
        if self.verbose:
            for file in files: self.vs("os.rmdir(%s)" % file)
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            os.rmdir(file)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res

class creat(metaop):
    """create a list of files by os.open() and os.close() pairs"""
    def __init__(self, files, flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 
        mode=0600, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "creat", files, verbose, dryrun)
        self.flags = flags
        self.mode = mode
    
    def exec(self):
        if self.verbose:
            for file in files:
                self.vs("os.close(os.open(%s, 0x%x, 0x%x))" 
                    % (file, flags, mode))
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            os.close(os.open(file, flags, mode))
            self.res.append((s, timer()))

        assert len(self.res) == len(self.files)
        return self.res

class access(metaop):
    """access a list of files by os.access()"""
    def __init__(self, files, mode=os.F_OK, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "access", files, verbose, dryrun)
        self.mode = mode

    def exec(self):
        if self.verbose:
            for file in files:
                self.vs("os.access(%s, 0x%x)" % (file, mode))
        if self.dryrun: return None
        
        for file in files:
            s = timer()
            ret = os.access(file, mode)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res

class open(metaop):
    """open a list of files by os.open()"""
    def __init__(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False,
        **kw):
        metaop.__init__(self, "open", files, verbose, dryrun)
        self.flags = flags

    def exec(self):
        if self.verbose:
            for file in self.files:
                self.vs("os.open(%s, 0x%x)" % (file, flags))
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            fd = os.open(file, flags)
            self.res.append((s, timer()))
            os.close(fd)
        
        assert len(self.res) == len(self.files)
        return self.res

class open_close(metaop):
    """access a list of files by os.open() and os.close() pairs"""
    def __init__(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False,
        **kw):
        metaop.__init__(self, "open_close", files, verbose, dryrun)
        self.flags = flags
        
        if self.verbose:
            for file in self.files:
                self.vs("os.close(os.open(%s, 0x%x))" % (file, flags))
        if self.dryrun: return None
            
        for file in self.files:
            s = timer()
            os.close(os.open(file, flags))
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res

class stat_exist(metaop):
    """access a list of files by os.stat()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "stat_exist", files, verbose, dryrun)

    def exec(self):
        if self.verbose:
            for file in self.files: self.vs("os.stat(%s)" % file)
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            os.stat(file)
            self.res.append((s, timer()))

        assert len(self.res) == len(self.files)
        return self.res 

class stat_non(metaop):
    """access a list of NON-EXIST files by os.stat()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "stat_non", files, verbose, dryrun)

    def exec(self):
        nfiles = map(lambda f:f+'n', self.files)
        if self.verbose:
            for file in nfiles: self.vs("os.stat(%s)" % file)
        if self.dryrun: return None
        
        for file in nfiles:
            s = timer()
            try: os.stat(file)
            except: pass
            self.res.append((s, timer()))

        assert len(self.res) == len(nfiles)
        return self.res

class utime(metaop):
    """access a list of files by os.utime()"""
    def __init__(self, files, times=None, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "utime", files, verbose, dryrun)
        self.times = times

   def exec(self):
        if self.verbose:
            for file in self.files: self.vs("os.utime(%s, %s)" % (file, times))
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            os.utime(file, self.times)
            self.res.append((s, timer()))
        
        assert len(self.res) == len(self.files)
        return self.res 

class chmod(metaop):
    """access a list of files by os.chmod()"""
    def __init__(self, files, mode=stat.S_IEXEC, verbose=False, dryrun=False,
        **kw):
        metaop.__init__(self, "chmod", files, verbose, dryrun)
   
    def exec(self):
        if self.verbose:
            for file in self.files:
                self.vs("os.chmod(%s, 0x%x)" % (file, mode))
        if self.dryrun: return None
           
        for file in self.files:
            s = timer()
            os.chmod(file, self.mode)
            self.res.append((s, timer()))

        assert len(self.res) == len(self.files)
        return self.res 

class rename(metaop):
    """access a list of files by os.rename()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        metaop.__init__(self, "rename", files, verbose, dryrun)

    def exec(self):
        if self.verbose:
            for file in self.files:
                self.vs("os.rename(%s, %s.to)" % (file, file))
        if self.dryrun: return None
        
        for file in self.files:
            tofile = file + ".to"
            s = timer()
            os.rename(file, tofile)
            self.res.append((s, timer()))
        assert len(self.res) == len(self.files)
        
        # rename back
        for file in self.files:
            tofile = file + ".to"
            if self.verbose: self.vs("os.rename(%s, %s) back" % (tofile, file))
            os.rename(tofile, file)

        return self.res

class unlink(metaop):
    """unlink a list of files by os.unlink()"""
    def __init__(self, files, self.verbose=False, self.dryrun=False, **kw):
        metaop.__init__(self, "unlink", files, verbose, dryrun)
    
    def exec(op):
        if self.verbose:
            for file in self.files: self.vs("os.unlink(%s)" % file)
        if self.dryrun: return None
        
        for file in self.files:
            s = timer()
            os.unlink(file)
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
        self.res = []
    
    def vs(self, msg):
        sys.stdout.write("%s\n" % msg)

    def exec(self):
        sys.stdout.write("noop\n")
        return None

class read(ioop):
    """read a file by os.read() with give parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_RDONLY, verbose=False,
        dryrun=False, **kw):
        ioop.__init__(self, "read", file, fsize, blksize, flags, verbose,
        dryrun)
    
    def exec(self)
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
    """reread a file by os.read() with given parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_RDONLY, verbose=False, 
        dryrun=False, **kw):
        ioop.__init__(self, "reread", file, fsize, blksize, flags, verbose,
        dryrun)

    def exec(self):
        if self.verbose:
            self.vs("os.read(%s, %d) * %d" % (file, blksize, fsize/blksize))
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
        mode=0600, byte='0', fsync=False, verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "write", file, fsize, blksize, flags, verbose,
        dryrun)
        self.mode = mode
        self.byte = byte
        self.fsync = fsync
       
    def exec(self):
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
    """re-write a file by os.write() with given parameters"""
    def __init__(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR,
        mode=0600, byte='1', fsync=False, verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "rewrite", file, fsize, blksize, flags, verbose,
        dryrun)
        self.mode = mode
        self.byte = byte
        self.fsync = fsync

    def exec(self):
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
    """read a file by f.read() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='r', verbose=False,
        dryrun=False, **kw):
        ioop.__init__(self, "fread", file, fsize, blksize, flags, verbose,
        dryrun)
    
    def exec(self):
        if self.verbose:
            self.vs("f.read(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        ret = 1
        s = timer()
        f = open(self.file, self.flags)
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
    """read a file by f.read() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='r', verbose=False,
        dryrun=False, **kw):
        ioop.__init__(self, "freread", file, fsize, blksize, flags, verbose,
        dryrun)
    
    def exec(self):
        if self.verbose:
            self.vs("f.read(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        ret = 1
        s = timer()
        f = open(self.file, self.flags)
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
    
    def exec(self):
        if self.verbose:
            self.vs("f.write(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        f = open(self.file, self.flags)
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
    """re-write a file by f.write() with given parameters"""
    def __init__(self, file, fsize, blksize, flags='w', byte='3', fsync=False,
        verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "frewrite", file, fsize, blksize, flags, verbose,
        dryrun)
        self.byte = byte
        self.fsync = fsync
    
    def exec(self):
        if self.verbose:
            self.vs("f.write(%s, %d) * %d" 
                % (self.file, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        f = open(self.file, self.flags)
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
    
    def exec(self):
        # Generate distribution
        randwalk = []
        opcnt = self.fsize / self.blksize
        if dist is None or dist[0] == "random":
            for i in range(0, opcnt):
                offset = random.randint(0, fsize)
                assert offset > 0 and offset < fsize
                randwalk.append(offset)
        elif dist[0] == "normal":
            dist_name, dist_mu, dist_sigma = dist
            # TODO: Calculate mu and sigma
            # if mu is None: mu = 
            # if sigma is None: sigma = 
            for i in range(0, opcnt):
                offset = int(round(normalvariate(dist_mu, dist_sigma)))
                assert offset > 0 and offset < fsize
                randwalk.append(offset)
        
        if self.verbose:
            for offset in randwalk:
                self.self.verbose("os.read(%s, %d) * %d at %d" %
                    (self.file, self.blksize, self.fsize/self.blksize), offset)
        if self.dryrun: return None
        
        s = timer()
        fd = os.open(self.file, self.flags)
        self.res.append((s, timer()))
        for offset in randwalk:
            s = timer()
            os.lseek(fd, offset, os.SEEK_SET)
            ret = os.read(fd, self.blksize)
            self.res.append((s, timer()))
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class offsetwrite(ioop):
    """Write a file by os.write() with offsets in a certain distribution"""
    def __init__(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR, 
        byte='4', dist=None, verbose=False, dryrun=False, **kw):
        ioop.__init__(self, "offsetwrite", file, fsize, blksize, flags, 
        verbose, dryrun)
        self.byte = byte
        self.dist = dist

    def exec(self):
        # Generate distribution
        randwalk = []
        opcnt = fsize / blksize
        if dist is None or dist[0] == "random":
            for i in range(0, opcnt):
                offset = random.randint(0, fsize)
                assert offset > 0 and offset < fsize
                randwalk.append(offset)
        elif dist[0] == "normal":
            dist_name, dist_mu, dist_sigma = dist
            # TODO: Calculate mu and sigma
            # if mu is None: mu = 
            # if sigma is None: sigma = 
            for i in range(0, opcnt):
                offset = int(round(normalvariate(dist_mu, dist_sigma)))
                assert offset > 0 and offset < fsize
                randwalk.append(offset)
        
        if self.verbose:
            for offset in randwalk:
                self.self.verbose("os.write(%s, %d) * %d at %d" %
                    (self.file, self.blksize, self.fsize/self.blksize), offset)
        if self.dryrun: return None

        block = self.byte * self.blksize
        s = timer()
        fd = os.open(self.file, self.flags)
        self.res.append((s, timer()))
        for offset in randwalk:
            s = timer()
            os.lseek(fd, offset, os.SEEK_SET)
            ret = os.write(fd, block)
            self.res.append((s, timer()))
        if self.fsync:
            s = timer()
            os.fsync(fd)
            self.res.append((s, timer()))
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res
