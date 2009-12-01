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

class MetaOper:
    """
    Filesystem metadata operations
    
    Primitives to conduct a filesystem metadata operation
    Input: a list of files to be manipulated on
    Output: operation name and a list of pairs of operation start and end time
    """
    def vs(self, msg):
        """verbose output"""
        sys.stout.write("\n" % msg)

    def mkdir(self, dirs, verbose=False, dryrun=False):
        """make a list of directories by os.mkdir()"""
        if verbose:
            for dir in dirs: self.vs("os.mkdir(%s)" % dir)
        if dryrun: return None
        
        res = []
        for dir in dirs:
            s = timer()
            os.mkdir(dir)
            res.append((s,timer()))
        
        assert len(res) == len(dirs)
        return ("mkdir", res)
    
    def rmdir(self, dirs, verbose=False, dryrun=False):
        """remove a list of directories by os.rmdir()"""
        if verbose:
            for dir in dirs: self.vs("os.rmdir(%s)" % dir)
        if dryrun: return None
        
        res = []
        for dir in dirs:
            s = timer()
            os.rmdir(dir)
            res.append((s,timer()))
        
        assert len(res) == len(dirs)
        return ('rmdir', res)
    
    def creat(self, files, flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 
        mode=0600, verbose=False, dryrun=False):
        """create a list of files by os.open() and os.close() pairs"""
    
        if verbose:
            for file in files:
                self.vs("os.close(os.open(%s, 0x%x, 0x%x))" 
                    % (file, flags, mode))
        if dryrun: return None
        
        res = []
        for file in files:
            s = timer()
            os.close(os.open(file, flags, mode))
            res.append((s,timer()))

        assert len(res) == len(files)
        return ('creat', res) 
    
    def access(self, files, mode=os.F_OK, verbose=False, dryrun=False):
        """access a list of files by os.access()"""
        
        if verbose:
            for file in files:
                self.vs("os.access(%s, 0x%x)" % (file, mode))
        if dryrun: return None
        
        res = []
        for file in files:
            s = timer()
            ret = os.access(file, mode)
            res.append((s,timer()))
        
        assert len(res) == len(files)
        return ('access', res) 
    
    def open(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False):
        """open a list of files by os.open()"""
        
        if verbose:
            for file in files:
                self.vs("os.open(%s, 0x%x)" % (file, flags))
        if dryrun: return None
        
        res = []
        for file in files:
            s = timer()
            fd = os.open(file, flags)
            res.append((s,timer()))
            os.close(fd)
        
        assert len(res) == len(files)
        return ('open', res) 
    
    def open_close(self, files, flags=os.O_RDONLY, verbose=False,
        dryrun=False):
        """access a list of files by os.open() and os.close() pairs"""

        if verbose:
            for file in files:
                self.vs("os.close(os.open(%s, 0x%x))" % (file, flags))
        if dryrun: return None
            
        res = []
        for file in files:
            s = timer()
            os.close(os.open(file, flags))
            res.append((s, timer()))
        
        assert len(res) == len(files)
        return ('open+close', res) 
    
    def stat_exist(self, files, verbose=False, dryrun=False):
        """access a list of files by os.stat()"""
        
        if verbose:
            for file in files: self.vs("os.stat(%s)" % file)
        if dryrun: return None
        
        res = []
        for file in files:
            s = timer()
            os.stat(file)
            res.append((s, timer()))

        assert len(res) == len(files)
        return ('stat_exist', res) 
    
    def stat_non(self, files, verbose=False, dryrun=False):
        """access a list of NON-EXIST files by os.stat()"""
        
        nfiles = map(lambda f:f+'n', files)
        if verbose:
            for file in nfiles: self.vs("os.stat(%s)" % file)
        if dryrun: return None
        
        res = []
        for file in nfiles:
            s = timer()
            try: os.stat(file)
            except: pass
            res.append((s,timer()))

        assert len(res) == len(nfiles)
        return ('stat_NONEXIST', res) 
        
    def utime(self, files, times=None, verbose=False, dryrun=False):
        """access a list of files by os.utime()"""
        
        if verbose:
            for file in files: self.vs("os.utime(%s, %s)" % (file, times))
        if dryrun: return None
        
        res = []
        for file in files:
            s = timer()
            os.utime(file, times)
            res.append((s, timer()))
        
        assert len(res) == len(files)
        return ('utime', res) 
        
    def chmod(self, files, mode=stat.S_IEXEC, verbose=False, dryrun=False):
        """access a list of files by os.chmod()"""
        
        if verbose:
            for file in files: self.vs("os.chmod(%s, 0x%x)" % (file, mode))
        if dryrun: return None
           
        res = []
        for file in files:
            s = timer()
            os.chmod(file, mode)
            res.append((s,timer()))

        assert len(res) == len(files)
        return ('chmod', res) 
        
    def rename(self, files, verbose=False, dryrun=False):
        """access a list of files by os.rename()"""
        
        if verbose:
            for file in files:
                self.vs("os.rename(%s, %s.to)" % (file, file))
        if dryrun: return None
        
        res = []
        for file in files:
            tofile = file + ".to"
            s = timer()
            os.rename(file, tofile)
            res.append((s,timer()))
        assert len(res) == len(files)
        
        # rename back
        for file in files:
            tofile = file + ".to"
            if verbose: self.vs("os.rename(%s, %s) back" % (tofile, file))
            os.rename(tofile, file)

        return ('rename', res) 

    def unlink(self, files, verbose=False, dryrun=False):
        """unlink a list of files by os.unlink()"""
        
        if verbose:
            for file in files: self.vs("os.unlink(%s)" % file)
        if dryrun: return None
        
        res = []
        for file in files:
            s = timer()
            os.unlink(file)
            res.append((s,timer()))
        
        assert len(res) == len(files)
        return ('unlink', res) 

class IOOper:
    """
    Filesystem input/output operations
    
    Primitives to conduct a filesystem I/O operation
    Input: a file to be manipulated on
    Output: operation name and a list of pairs of operation start and end time
    """
    def __init__(self):
        random.seed() # For offsetread/write

    def vs(self, msg):
        """verbose output"""
        sys.stout.write("\n" % msg)
    
    def read(self, file, fsize, blksize, flags=os.O_RDONLY, verbose=False,
        dryrun=False):
        """read a file by os.read() with give parameters"""
        
        if verbose:
            self.vs("os.read(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        # Be careful!
        # The first record is open time and the last record is close time
        res = []
        ret = 1
        s = timer()
        fd = os.open(file, flags)
        res.append((s,timer()))
        while ret:
            s = timer()
            ret = os.read(fd, blksize)
            res.append((s,timer()))
            #assert len(ret) == blksize
        s = timer()
        os.close(fd)
        res.append((s,timer()))

        return ('read', res)
    
    def reread(self, file, fsize, blksize, flags=os.O_RDONLY, verbose, dryrun):
        """re-read a file by os.read() with given parameters"""

        if verbose:
            self.vs("os.read(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None

        res = []
        ret = 1
        s = timer()
        fd = os.open(file, flags)
        t = timer()
        res.append((s,t))
        while ret:
            s = timer()
            ret = os.read(fd, blksize)
            res.append((s,timer()))
            #assert len(ret) == blksize
        s = timer()
        os.close(fd)
        res.append((s,timer()))

        return ('reread', res)
    
    def write(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR, 
        mode=0600, byte='0', fsync=False, verbose=False, dryrun=False):
        """write a file by os.write() with given parameters"""
        
        if verbose:
            self.vs("os.write(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        block = byte * blksize
        writebytes = 0
        res = []
        s = timer()
        fd = os.open(file, flags, mode)
        res.append((s,timer()))
        while writebytes < fsize:
            s = timer()
            ret = os.write(fd, block)
            res.append((s,timer()))
            assert ret == blksize
            writebytes += ret
        if fsync:
            s = timer()
            os.fsync(fd)
            res.append((s,timer()))
        s = timer()
        os.close(fd)
        res.append((s,timer()))

        return ('write', res)
    
    def rewrite(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR, 
        mode=0600, byte='1', fsync=False, verbose=False, dryrun=False):
        """re-write a file by os.write() with given parameters"""
        
        if verbose:
            self.vs("os.write(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        block = byte * blksize
        writebytes = 0
        res = []
        s = timer()
        fd = os.open(file, flags, mode)
        res.append((s,timer()))
        while writebytes < fsize:
            s = timer()
            ret = os.write(fd, block)
            res.append((s,timer()))
            assert ret == blksize
            writebytes += ret
        if fsync:
            s = timer()
            os.fsync(fd)
            res.append((s,timer()))
        s = timer()
        os.close(fd)
        res.append((s,timer()))

        return ('rewrite', res)
    
    def fread(self, file, fsize, blksize, flags='r', verbose=False,
        dryrun=False):
        """read a file by f.read() with given parameters"""
        
        if verbose >= VERBOSE_INFO:
            self.vs("f.read(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        res = []
        ret = 1
        s = timer()
        f = open(file, flags)
        res.append((s,timer()))
        while ret:
            s = timer()
            ret = f.read(blksize)
            res.append((s,timer()))
            #assert len(ret) == blksize
        s = timer()
        f.close()
        res.append((s,timer()))

        return ('fread', res)
    
    def freread(self, file, fsize, blksize, flags='r', verbose=False,
        dryrun=False):
        """read a file by f.read() with given parameters"""
        
        if verbose:
            self.vs("f.read(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        res = []
        ret = 1
        s = timer()
        f = open(file, flags)
        res.append((s,timer()))
        while ret:
            s = timer()
            ret = f.read(blksize)
            res.append((s,timer()))
            #assert len(ret) == blksize
            opcnt += 1
        s = timer()
        f.close()
        res.append((s,timer()))

        return ('freread', res)

    def fwrite(self, file, fsize, blksize, flags='w', byte='2', fsync=False,
        verbose=False, dryrun=False):
        """write a file by f.write() with given parameters"""
        
        if verbose:
            self.vs("f.write(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        block = byte * blksize
        writebytes = 0
        res = []
        s = timer()
        f = open(file, flags)
        res.append((s,timer()))
        while writebytes < fsize:
            s = timer()
            f.write(block)
            res.append((s,timer()))
            assert ret == blksize
            writebytes += ret
        if fsync:
            s = timer()
            f.flush()
            os.fsync(f.fileno())
            res.append((s,timer()))
        s = timer()
        f.close()
        res.append((s,timer()))

        return ('fwrite', res) 
    
    def frewrite(self, file, fsize, blksize, flags='w', byte='3', fsync=False,
        verbose=False, dryrun=False):
        """re-write a file by f.write() with given parameters"""
        
        if verbose:
            self.vs("f.write(%s, %d) * %d" % (file, blksize, fsize/blksize))
        if dryrun: return None
        
        block = byte * blksize
        writebytes = 0
        res = []
        s = timer()
        f = open(file, flags)
        res.append((s,timer()))
        while writebytes < fsize:
            s = timer()
            f.write(block)
            res.append((s,timer()))
            assert ret == blksize
            writebytes += ret
        if fsync:
            s = timer()
            f.flush()
            os.fsync(f.fileno())
            res.append((s,timer()))
        s = timer()
        f.close()
        res.append((s,timer()))

        return ('frewrite', res)

    def offsetread(self, file, fsize, blksize, flags=os.O_RDONLY, 
        dist=None, verbose=False, dryrun=False):
        """Read a file by os.read() with offsets in a certain distribution
        
        dist -- a tuple (dist_name, dist_mu, dist_sigma)
        """

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
                offset = int(round(normalvariate(mu, sigma)))
                assert offset > 0 and offset < fsize
                randwalk.append(offset)
        
        if verbose:
            for offset in randwalk:
                self.verbose("os.read(%s, %d) * %d at %d" %
                    (file, blksize, fsize/blksize), offset)
        if dryrun: return None
        
        res = []
        s = timer()
        fd = os.open(file, flags)
        res.append((s,timer()))
        for offset in randwalk:
            s = timer()
            os.lseek(fd, offset, os.SEEK_SET)
            ret = os.read(fd, blksize)
            res.append((s,timer()))
        s = timer()
        os.close(fd)
        res.append((s,timer()))

        return ('offsetread', res)
    
    def offsetwrite(self, file, fsize, blksize, flags=os.O_CREAT | os.O_RDWR, 
        byte='4', dist=None, verbose=False, dryrun=False):
        """write a file by os.write() with offsets in a certain distribution"""
        
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
                offset = int(round(normalvariate(mu, sigma)))
                assert offset > 0 and offset < fsize
                randwalk.append(offset)
        
        if verbose:
            for offset in randwalk:
                self.verbose("os.write(%s, %d) * %d at %d" %
                    (file, blksize, fsize/blksize), offset)
        if dryrun: return None

        block = byte * blksize
        res = []
        s = timer()
        fd = os.open(file, flags)
        res.append((s, timer()))
        for offset in randwalk:
            s = timer()
            os.lseek(fd, offset, os.SEEK_SET)
            ret = os.write(fd, block)
            res.append((s, timer()))
        if fsync:
            s = timer()
            os.fsync(fd)
            res.append((s,timer()))
        s = timer()
        os.close(fd)
        res.append((s,timer()))

        return ('offsetwrite', res)
    
class FileSystemOperation(MetaOper, IOOper):
    def __init__(self):
        IOOper.__init__(self)
        
        # verbose
        self.verbosecnt = 0

    # verbose routines
    def vs(self, msg):
        """overrided vs()"""
        sys.stdout.write("[%05d] %s\n" % (self.verbosecnt, msg))
        self.verbosecnt += 1
