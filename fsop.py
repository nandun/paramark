#############################################################################
# ParaMark: High Fidelity Parallel File System Benchmark
# Copyright (C) 2009  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
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
# File System Operation Primitives
#   * Workload synthetic
#   * File I/O and metadata operations
#   * File data distribution
#

import os
import sys
import stat
import random

from common import *

__all__ = ["FileSystemOperation"]

class FileSystemOperation:
    """
    File system operation:
        * Workload synthetic
        * File system I/O and metadata primitives
    """
    def __init__(self, opts=None, **kw):
        # configuration by opts or **kw
        self.wdir = None    # working directory
        
        self.opcnt = None   # meta operation count
        self.factor = None
        self.fsize = None
        self.blksize = None
        self.syncio= False
        self.fsync = False
        self.opentime = True
        self.closetime = True
        
        self.dryrun = False
        self.verbosity = 0

        _FileSystemOperation_restrict = ["wdir", "opcnt", "factor", "fsize",
            "blksize", "syncio", "fsync", "opentime", "closetime", "dryrun",
            "verbosity"]
        update_opts_kw(self.__dict__, _FileSystemOperation_restrict,
            opts, kw)
        
        # workload files and dirs
        self.tempdir = None
        self.file = None
        self.files = None
        self.dir = None
        self.dirs = None

        # read-only variables
        self.op = {}
        self.op["mkdir"] = self.mkdir
        self.op["rmdir"] = self.rmdir
        self.op["creat"] = self.creat
        self.op["access"] = self.access
        self.op["open"] = self.open
        self.op["open+close"] = self.open_close
        self.op["stat"] = self.stat
        self.op["stat_NONEXIST"] = self.stat_non
        self.op["utime"] = self.utime
        self.op["chmod"] = self.chmod
        self.op["rename"] = self.rename
        self.op["unlink"] = self.unlink
        self.op["read"] = self.read
        self.op["reread"] = self.reread
        self.op["write"] = self.write
        self.op["rewrite"] = self.rewrite
        self.op["fread"] = self.fread
        self.op["freread"] = self.freread
        self.op["fwrite"] = self.fwrite
        self.op["frewrite"] = self.frewrite
        self.op["randread"] = self.randread
        self.op["randwrite"] = self.randwrite
        
        # verbose
        self.verbosecnt = 0
        
        # stream
        self.ws = sys.stdout.write
        self.es = sys.stderr.write
        
        random.seed()

    # verbose routines
    def verbose(self, msg):
        self.ws("[%05d] %s\n" % (self.verbosecnt, msg))
        self.verbosecnt += 1
    
    # 
    # Workload Generation
    #
    def gen_tempdir(self, prefix):
        self.tempdir = "%s/%s-%03d" % \
            (self.wdir, prefix, random.randint(0, 999))
        return self.tempdir
    
    def gen_dirs(self, num, factor=16):
        assert num > 0
        self.dirs = []
        queue = [ self.tempdir ]
        i = l = 0
        while i < num:
            if i % factor == 0:
                parent = queue.pop(0)
                l = l + 1
            child = "%s/L%d-%d" % (parent, l, i)
            self.dirs.append(child)
            queue.append(child)
            i = i + 1

        self.dir = self.dirs[0]
        return self.dirs
    
    def gen_files(self, num):
        assert num > 0
        self.files = []
        for i in range(0, num): 
            self.files.append("%s/%d.dat" % (self.tempdir, i))

        self.file = self.files[0]
        return self.files
    
    def shuffle_files_and_dirs(self, shuffle="random", round=1):
        if self.dirs is not None:
            if shuffle == "random":
                random.shuffle(self.dirs)
            elif shuffle == "round":
                for i in range(0, round):
                    self.dirs.append(self.dirs.pop(0))
            self.dir = self.dirs[0]
        
        if self.files is not None:
            if shuffle == "random":
                random.shuffle(self.files)
            elif shuffle == "round":
                for i in range(0, round):
                    self.files.append(self.files.pop(0))
            self.file = self.files[0]

        return (self.dir, self.dirs, self.file, self.files)
    
    # 
    # I/O and metadata primitives
    #
    # arguments are optional, this is useful for opList
    # return a 7-tuple
    # (opName, opCount, minopTime, maxopTime, elapsedTime, startTime, endTime)
    #
    def mkdir(self, dirs=None):
        """mkdir""" # special purpose, do not modify
        minop = INTEGER_MAX 
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if dirs is None:
            dirs = self.dirs

        if self.verbosity >= VERBOSE_INFO:
            for dir in dirs:
                self.verbose("mkdir: os.mkdir(%s)" % dir)
        if self.dryrun:
            return None
        
        start = timer()
        for dir in dirs:
            optick = timer()
            os.mkdir(dir)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        
        assert opcnt == len(dirs)
        return ('mkdir', opcnt, minop, maxop, elapsed, start, end)

    def rmdir(self, dirs=None):
        """rmdir""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if dirs is None:
            dirs = list(self.dirs)
            dirs.reverse()
        
        if self.verbosity >= VERBOSE_INFO:
            for dir in dirs:
                self.verbose("rmdir: os.rmdir(%s)" % dir)
        if self.dryrun:
            return None
        
        start = timer()
        for dir in dirs:
            optick = timer()
            os.rmdir(dir)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        
        assert opcnt == len(dirs)
        return ('rmdir', opcnt, minop, maxop, elapsed, start, end)

    def creat(self, files=None, flags=None, mode=0600):
        """creat""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
    
        if files is None:
            files = self.files
        if flags is None:
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        
        if self.verbosity >=  VERBOSE_INFO:
            for file in files:
                self.verbose("creat: os.open(%s, 0x%x, 0x%x)"
                             % (file, flags, mode))
        if self.dryrun:
            return None
        
        start = timer()
        for file in files:
            optick = timer()
            fd = os.open(file, flags, mode)
            os.close(fd)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()

        if self.dryrun:
            return None
        
        assert opcnt == len(files)
        return ('creat', opcnt, minop, maxop, elapsed, start, end) 

    def access(self, files=None, mode=None):
        """access""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files
        if mode is None:
            mode = os.F_OK

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("access: os.access(%s, 0x%x)" % (file, mode))
        if self.dryrun:
            return None
        
        start = timer()
        for file in files:
            optick = timer()
            ret = os.access(file, mode)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        
        assert opcnt == len(files)
        return ('access', opcnt, minop, maxop, elapsed, start, end) 

    def open(self, files=None, flags=None):
        """open""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files
        if flags is None:
            flags = os.O_RDONLY

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("open: os.open(%s, 0x%x)" % (file, flags))
        if self.dryrun:
            return None

        start = timer()
        for file in files:
            optick = timer()
            fd = os.open(file, flags)
            optick = timer() - optick
            os.close(fd)
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        
        assert opcnt == len(files)
        return ('open', opcnt, minop, maxop, elapsed, start, end) 
    
    def open_close(self, files=None, flags=None):
        """open+close""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files
        if flags is None:
            flags = os.O_RDONLY

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("open+close: os.open(%s, 0x%x)" % (file, flags))
        if self.dryrun:
            return None
            
        start = timer()
        for file in files:
            optick = timer()
            fd = os.open(file, flags)
            os.close(fd)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        
        assert opcnt == len(files)
        return ('open+close', opcnt, minop, maxop, elapsed, start, end) 
    
    def stat(self, files=None):
        """stat""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("stat: os.stat(%s)" % file)
        if self.dryrun:
            return None
        
        start = timer()
        for file in files:
            optick = timer()
            os.stat(file)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()

        assert opcnt == len(files)
        return ('stat', opcnt, minop, maxop, elapsed, start, end) 
    
    def stat_non(self, files=None):
        """stat_NONEXIST""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = map(lambda f:f+'n', self.files)

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("stat_non: os.stat(%s)" % file)
        if self.dryrun:
            return None

        start = timer()
        for file in files:
            optick = timer()
            try:
                os.stat(file)
            except:
                pass
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()

        assert opcnt == len(files)
        return ('stat_NONEXIST', opcnt, minop, maxop, elapsed, start, end) 
        
    def utime(self, files=None, times=None):
        """utime""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("utime: os.utime(%s, %s)" % (file, times))
        if self.dryrun:
            return None
        
        start = timer()
        for file in files:
            optick = timer()
            os.utime(file, times)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer() 
        
        assert opcnt == len(files)
        return ('utime', opcnt, minop, maxop, elapsed, start, end) 
        
    def chmod(self, files=None, mode=None):
        """chmod""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files
        if mode is None:
            mode = stat.S_IEXEC

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("chmod: os.chmod(%s, 0x%x)" % (file, mode))
        if self.dryrun:
            return None
            
        start = timer()
        for file in files:
            optick = timer()
            os.chmod(file, mode)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed  += optick
            opcnt += 1
        end = timer()

        assert opcnt == len(files)
        return ('chmod', opcnt, minop, maxop, elapsed, start, end) 
        
    def rename(self, files=None):
        """rename""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files
        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("rename: os.rename(%s, %s.to)" % (file, file))
        if self.dryrun:
            return None

        start = timer()
        for file in files:
            tofile = file + ".to"
            optick = timer()
            os.rename(file, tofile)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        assert opcnt == len(files)
        
        # rename back
        for file in files:
            tofile = file + ".to"
            if self.verbosity >= VERBOSE_DETAILS:
                self.verbose("rename_back: os.rename(%s, %s)" % 
                             (tofile, file))
            os.rename(tofile, file)

        return ('rename', opcnt, minop, maxop, elapsed, start, end) 

    def unlink(self, files=None):
        """unlink""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if files is None:
            files = self.files

        if self.verbosity >= VERBOSE_INFO:
            for file in files:
                self.verbose("unlink: os.unlink(%s)" % file)
        if self.dryrun:
            return None
        
        start = timer()
        for file in files:
            optick = timer()
            os.unlink(file)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        
        assert opcnt == len(files)
        return ('unlink', opcnt, minop, maxop, elapsed, start, end) 

    def read(self, file=None, flags=None, fsize=None, blksize=None):
        """read""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0

        if file is None:
            file = self.file
        if flags is None:
            flags = os.O_RDONLY
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("read: os.read(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None

        start = timer()
        fd = os.open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            ret = os.read(fd, blksize)
            optick = timer() - optick
            assert len(ret) == blksize
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        os.close(fd)
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('read', opcnt, minop, maxop, elapsed, start, end)

    def reread(self, file=None, flags=None, fsize=None, blksize=None):
        """reread""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0

        if file is None:
            file = self.file
        if flags is None:
            flags = os.O_RDONLY
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("reread: os.read(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None

        start = timer()
        fd = os.open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            ret = os.read(fd, blksize)
            optick = timer() - optick
            assert len(ret) == blksize
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        os.close(fd)
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('reread', opcnt, minop, maxop, elapsed, start, end)
    
    def write(self, file=None, flags=None, mode=0600, fsize=None, blksize=None):
        """write""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDWR
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("write: os.write(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None
        
        block = '0' * blksize
        start = timer()
        fd = os.open(file, flags, mode)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            ret = os.write(fd, block)
            optick = timer() - optick
            assert ret == blksize
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        if self.fsync:
            os.fsync(fd)
            elapsed += timer() - end
            end = timer()
        os.close(fd)
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('write', opcnt, minop, maxop, elapsed, start, end)
    
    def rewrite(self, file=None, flags=None, mode=0600, fsize=None, blksize=None):
        """rewrite""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDWR
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("rewrite: os.write(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None
        
        block = '1' * blksize
        start = timer()
        fd = os.open(file, flags, mode)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            ret = os.write(fd, block)
            optick = timer() - optick
            assert ret == blksize
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        if self.fsync:
            os.fsync(fd)
            elapsed += timer() - end
            end = timer()
        os.close(fd)
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('rewrite', opcnt, minop, maxop, elapsed, start, end)
    
    def fread(self, file=None, flags='r', fsize=None, blksize=None):
        """fread""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("fread: f.read(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None
        
        start = timer()
        f = open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            ret = f.read(blksize)
            optick = timer() - optick
            assert len(ret) == blksize
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        f.close()
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('fread', opcnt, minop, maxop, elapsed, start, end)
    
    def freread(self, file=None, flags='r', fsize=None, blksize=None):
        """freread""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("freread: f.read(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None
        
        start = timer()
        f = open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            ret = f.read(blksize)
            optick = timer() - optick
            assert len(ret) == blksize
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        f.close()
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('freread', opcnt, minop, maxop, elapsed, start, end)

    def fwrite(self, file=None, flags='w', fsize=None, blksize=None):
        """fwrite""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("fwrite: f.write(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None
        
        block = '2' * blksize
        start = timer()
        f = open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            f.write(block)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        if self.fsync:
            f.flush()
            os.fsync(f.fileno())
            elapsed += timer() - end
            end = timer()
        f.close()
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('fwrite', opcnt, minop, maxop, elapsed, start, end)
    
    def frewrite(self, file=None, flags='w', fsize=None, blksize=None):
        """frewrite""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_INFO:
            self.verbose("frewrite: f.write(%s, %d) * %d" %
                (file, blksize, fsize/blksize))
        if self.dryrun:
            return None
        
        block = '3' * blksize
        start = timer()
        f = open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        while opcnt * blksize < fsize:
            optick = timer()
            f.write(block)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
            opcnt += 1
        end = timer()
        if self.fsync:
            f.flush()
            os.fsync(f.fileno())
            elapsed += timer() - end
            end = timer()
        f.close()
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('frewrite', opcnt, minop, maxop, elapsed, start, end)

    def randread(self, file=None, flags=None, fsize=None, blksize=None):
        """randread""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDONLY
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        randwalk = []
        while opcnt * blksize < fsize:
            randwalk.append(random.randint(0, fsize))
            opcnt += 1
        
        if self.verbosity >= VERBOSE_INFO and self.verbosity < VERBOSE_DETAILS:
            self.verbose("randread: os.lseek->os.read(%s, %d) * %d" %
                    (file, blksize, fsize/blksize))
        
        if self.verbosity >= VERBOSE_DETAILS:
            for offset in randwalk:
                self.verbose("randread: os.lseek(%s, %d, os.SEEK_SET)\n" %
                    (file, offset))
                self.verbose("randread: os.read(%s, %d) * %d" %
                    (file, blksize, fsize/blksize))
        
        if self.dryrun:
            return None
        
        start = timer()
        fd = os.open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        for offset in randwalk:
            optick = timer()
            # os.lseek(fd, offset, os.SEEK_SET)
            os.lseek(fd, offset, 0) # back compatibility
            ret = os.read(fd, blksize)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
        end = timer()
        os.close(fd)
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('randread', opcnt, minop, maxop, elapsed, start, end)
    
    def randwrite(self, file=None, flags=None, fsize=None, blksize=None):
        """randwrite""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDWR
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        randwalk = []
        while opcnt * blksize < fsize:
            randwalk.append(random.randint(0, fsize))
            opcnt += 1
        
        if self.verbosity >= VERBOSE_INFO and self.verbosity < VERBOSE_DETAILS:
            self.verbose("randwrite: os.lseek->os.write(%s, %d) * %d" %
                    (file, blksize, fsize/blksize))
        
        if self.verbosity >= VERBOSE_DETAILS:
            for offset in randwalk:
                self.verbose("randwrite: os.lseek(%s, %d, os.SEEK_SET)\n" %
                    (file, offset))
                self.verbose("randwrite: os.write(%s, %d) * %d" %
                    (file, blksize, fsize/blksize))
        
        if self.dryrun:
            return None

        block = '4' * blksize
        start = timer()
        fd = os.open(file, flags)
        if self.opentime:
            elapsed = timer() - start
        for offset in randwalk:
            optick = timer()
            # os.lseek(fd, offset, os.SEEK_SET)
            os.lseek(fd, offset, 0) # back compatibility
            ret = os.write(fd, block)
            optick = timer() - optick
            minop = min(minop, optick)
            maxop = max(maxop, optick)
            elapsed += optick
        end = timer()
        os.close(fd)
        if self.closetime:
            elapsed += timer() - end
            end = timer()

        return ('randwrite', opcnt, minop, maxop, elapsed, start, end)
