#!/usr/bin/env python

#############################################################################
# ParaMark: High Fidelity File System Benchmark
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

#############################################
# main classes of basic benchmark
#############################################

import csv
import errno
import optparse
import os
import pwd
import random
import shutil
import socket
import stat
import sys
import textwrap
import time

from common import *

all = ["DataGenerator", "TestSet", "BaseBenchmark"]

class DataGenerator:
    def __init__(self, base):
        self.base = base
        self.dirs = None
        self.files = None
        self.dir = None
        self.file = None
        self.tempdir = None
        random.seed()
    
    def gentempdir(self):
        self.tempdir = "%s/paramark-%s-%d-%03d" % \
                       (self.base, socket.gethostname(), os.getpid(),
                       random.randint(0, 999))

    def gendirs(self, num, factor=16):
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

    def genfiles(self, num):
        assert num > 0
        self.files = []
        for i in range(0, num): 
            self.files.append("%s/%d.dat" % (self.tempdir, i))

        self.file = self.files[0]
        return self.files

    def shuffle(self, shuffle="random", round=1):
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
    
    def clean(self):
        shutil.rmtree(self.tempdir)

class TestSet:
    def __init__(self):
        self.id = None  # hash value of following arguments: env and ops
        self.env = {}   # configuration of current test
        self.ops = []   # list of operations to perform
        self.dat = []   # raw output from oplist
    
class BaseBenchmark:
    """ main class that performs the file system benchmark """
    def __init__(self, opts=None, **kw):
        ## read only variable, Do NOT modify ##
        self.opdict = {}
        self.opdict["mkdir"] = self.mkdir
        self.opdict["rmdir"] = self.rmdir
        self.opdict["creat"] = self.creat
        self.opdict["access"] = self.access
        self.opdict["open"] = self.open
        self.opdict["open+close"] = self.open_close
        self.opdict["stat"] = self.stat
        self.opdict["stat_NONEXIST"] = self.stat_non
        self.opdict["utime"] = self.utime
        self.opdict["chmod"] = self.chmod
        self.opdict["rename"] = self.rename
        self.opdict["unlink"] = self.unlink
        self.opdict["read"] = self.read
        self.opdict["reread"] = self.reread
        self.opdict["write"] = self.write
        self.opdict["rewrite"] = self.rewrite
        self.opdict["fread"] = self.fread
        self.opdict["freread"] = self.freread
        self.opdict["fwrite"] = self.fwrite
        self.opdict["frewrite"] = self.frewrite
        self.opdict["randread"] = self.randread
        self.opdict["randwrite"] = self.randwrite
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        
        ## configuration variables ##
        self.cmd = None
        self.mode = None
        self.wdir = None
        self.nproc = 1
        self.unit = None
        self.syncio = False
        self.fsync = False
        self.shuffle = None 
        self.round = 1
        self.opentime = True
        self.closetime = True
        self.sleep = 0.0
        self.verbosity = 0
        self.keep = False
        self.dryrun = False
        self.fsizerange = None
        self.blksizerange = None
        self.opcntrange = None
        self.factorrange = None
        self.metaops = None
        self.ioops = None
        self.unit = None
        self.tests = []
        self.reportdir = None
        self.reportsmartsize = None
        
        # initial from opts and **kw
        if opts is not None:
            for k, v in opts.__dict__.items():
                if self.__dict__.has_key(k):
                    self.__dict__[k] = v
        
        for k, v in kw.items():
            if self.__dict__.has_key(k):
                self.__dict__[k] = v
        
        # runtime variables
        self.start = None
        self.end = None
        self.optype = None
        self.opcnt = None
        self.factor = None
        self.fsize = None
        self.blksize = None
        self.wdat = DataGenerator(self.wdir)
        self.vcnt = 0

        # initial test set
        if self.mode == 'auto' or self.mode == 'io':
            for fsize in self.fsizerange:
                for blksize in self.blksizerange:
                    t = TestSet()
                    t.env['optype'] = 'io'
                    t.env['nproc'] = self.nproc
                    t.env['fsize'] = fsize
                    t.env['blksize'] = blksize
                    t.ops = self.ioops
                    t.id = string_hash("%r%r" % (t.env, t.ops))
                    self.tests.append(t)
        if self.mode == 'auto' or self.mode == 'meta':
            for opcnt in self.opcntrange:
                for factor in self.factorrange:
                    t = TestSet()
                    t.env['optype'] = 'meta'
                    t.env["nproc"] = self.nproc
                    t.env['opcnt'] = opcnt
                    t.env['factor'] = factor
                    t.ops = self.metaops
                    t.id = string_hash("%r%r" % (t.env, t.ops))
                    self.tests.append(t)
    
    def verbose(self, msg):
        ws("[%9s#%5d:%05d] %s\n" % (self.hostname, self.pid, self.vcnt, msg))
        self.vcnt += 1
    
    def ensuredir(self, path):
        if os.path.isdir(path):
            return 0
        
        if self.verbosity >= VERBOSE_CHECKPOINT:
            self.verbose("ensuredir: os.makedirs(%s)" % path)

        if self.dryrun:
            return 0
        try:
            os.makedirs(path)
        except OSError, err:
            if err.errno != errno.EEXIST or os.path.isfile(path):
                es("failed to create %s: %s\n" % \
                    (path, os.strerror(err.errno)))
                return -1
        return 0

    def run(self):
        self.start = (time.localtime(), timer())
        for test in self.tests:
            ops = self.preprocess(test)
            for op in ops:
                test.dat.append(op())
                self.interprocess()
            self.postprocess()
        self.end = (time.localtime(), timer())

    def preprocess(self, test=None):
        if test is not None:
            self.__dict__.update(test.env)
        
        self.wdat.gentempdir();
        if self.verbosity >= VERBOSE_FILE_DIR:
            self.verbose("preprocess: ensuredir(%s)" % self.wdat.tempdir)
        self.ensuredir(self.wdat.tempdir)
        
        if self.optype == 'io':
            if self.verbosity >= VERBOSE_FILE_DIR:
                self.verbose("preprocess: self.wdat.genfiles()")
            self.wdat.genfiles(self.nproc)
            return map(lambda x:self.opdict[x], self.ioops)
        if self.optype == 'meta':
            if self.verbosity >= VERBOSE_FILE_DIR:
                self.verbose("preprocess: self.wdat.gendirs()")
                self.verbose("preprocess: self.wdat.genfiles()")
            self.wdat.gendirs(self.opcnt, self.factor)
            self.wdat.genfiles(self.opcnt)
            return map(lambda x:self.opdict[x], self.metaops)
    
    def interprocess(self):
        if self.verbosity >= VERBOSE_CHECKPOINT:
            self.verbose("interprocess:\n")

        if self.verbosity >= 3:
            self.verbose("interprocessing: time.sleep(%f)" % self.sleep)
        if not self.dryrun and self.sleep > 0:
            time.sleep(self.sleep)

    def postprocess(self):
        # cleanup
        if self.verbosity >= 3:
            self.verbose("postprocess: self.wdat.clean()")
        if not self.dryrun and not self.keep:
            start = timer()
            self.wdat.clean()
            return ('cleanup_time', timer() - start)
        else:
            return ('cleanup_time', 0)
    
    ### reporing routines ###
    # output data in various format
    def reporttocsv(self, tests=None, reportdir=None):
        """aggregate result and store to csv file"""
        if tests is None:
            tests = self.tests
        if reportdir is None:
            reportdir = self.reportdir
        
        iof = open("%s/io.csv" % reportdir, "wb")
        metaf = open("%s/meta.csv" % reportdir, "wb")
        iocsv = csv.writer(iof)
        metacsv = csv.writer(metaf)
        
        # write csv header
        iocsv.writerow(["operation", "proc#", "filesize", "blocksize",
            "exectime", "mintime/call", "maxtime/call", "throughput"])
        metacsv.writerow(["operation", "proc#", "count", "factor", "exectime",
            "mintime/call", "maxtime/call", "throughput"])
        
        unit = eval(self.unit)
        for t in tests:
            if t.env["optype"] == "meta":
                for d in t.dat:
                    nproc = t.env["nproc"]
                    factor = t.env["factor"]
                    opname, opcnt, opmin, opmax, elapsed, start, end = d
                    row = [opname, nproc, opcnt, factor, elapsed, opmin, 
                        opmax, "%s" % (opcnt/elapsed)]
                    metacsv.writerow(row)
            elif t.env["optype"] == "io":
                for d in t.dat:
                    nproc = t.env["nproc"]
                    fsize = t.env["fsize"]
                    blksize = t.env["blksize"]
                    opname, opcnt, opmin, opmax, elapsed, start, end = d
                    row = [opname, nproc, "%s%s" % smart_datasize(fsize), 
                        "%s%s" % smart_datasize(blksize), elapsed, opmin,
                        opmax, "%s" % (fsize/elapsed/unit)]
                    iocsv.writerow(row)
        
        iof.close()
        metaf.close()

        ws(\
"""\
I/O performance data has been written to %s/io.csv
Metdata performance data has been written to %s/meta.csv

Currently ParaMark only provides raw data in csv format,
you may use Microsoft Excel or OpenOffice Calc
to import, filter and plot your data.
""" % (reportdir, reportdir))

    def reportstatus(self, stream=None):
        if stream is None:
            stream = sys.stdout

        stream.write(\
"""
ParaMark Base Benchmark (version %s, %s)
          platform: %s
         run began: %s
           run end: %s
          duration: %s seconds
              user: %s (%s)
           command: %s
 working directory: %s
              mode: %s

""" \
        % (PARAMARK_VERSION, PARAMARK_DATE,
           self.platform,
           time.strftime("%a, %d %b %Y %H:%M:%S %Z", self.start[0]),
           time.strftime("%a, %d %b %Y %H:%M:%S %Z", self.end[0]),
           self.end[1] - self.start[1],
           self.user, self.uid,
           self.cmd,
           self.wdir,
           self.mode))

        stream.flush()
        
    def report(self):
        if self.reportdir is None:
            self.reportdir = os.getcwd() + \
                time.strftime("/report-%H%M%S", self.start[0])
        else:
            self.reportdir = os.path.abspath(self.reportdir)

        try:
            os.makedirs(self.reportdir)
        except OSError:
            pass
        
        self.reportstatus()
        self.reporttocsv()

    ### I/O and metadata primitives ###
    # operation does not take arguments, this is useful for opList
    # return a 7-tuple
    # (opName, opCount, minopTime, maxopTime, elapsedTime, startTime, endTime)
    def mkdir(self, dirs=None):
        """mkdir""" # special purpose, do not modify
        minop = INTEGER_MAX 
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if dirs is None:
            dirs = self.wdat.dirs

        if self.verbosity >= VERBOSE_OP:
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
            dirs = list(self.wdat.dirs)
            dirs.reverse()
        
        if self.verbosity >= VERBOSE_OP:
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
            files = self.wdat.files
        if flags is None:
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        
        if self.verbosity >=  VERBOSE_OP:
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
            files = self.wdat.files
        if mode is None:
            mode = os.F_OK

        if self.verbosity >= VERBOSE_OP:
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
            files = self.wdat.files
        if flags is None:
            flags = os.O_RDONLY

        if self.verbosity >= VERBOSE_OP:
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
            files = self.wdat.files
        if flags is None:
            flags = os.O_RDONLY

        if self.verbosity >= VERBOSE_OP:
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
            files = self.wdat.files

        if self.verbosity >= VERBOSE_OP:
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
            files = map(lambda f:f+'n', self.wdat.files)

        if self.verbosity >= VERBOSE_OP:
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
            files = self.wdat.files

        if self.verbosity >= VERBOSE_OP:
            for file in files:
                self.verbose("utime: os.utime(%s, None)" % file)
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
            files = self.wdat.files
        if mode is None:
            mode = stat.S_IEXEC

        if self.verbosity >= VERBOSE_OP:
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
            files = self.wdat.files
        if self.verbosity >= VERBOSE_OP:
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
            if self.verbosity >= 3:
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
            files = self.wdat.files

        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if flags is None:
            flags = os.O_RDONLY
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if flags is None:
            flags = os.O_RDONLY
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDWR
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDWR
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.syncio:
            flags = flags | os.O_SYNC
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_OP:
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
            file = self.wdat.file
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
        
        if self.verbosity >= VERBOSE_OP:
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

    def randread(self, file=None, flags=None, fsize=None, blksize=None,
                 seed=None):
        """randread""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.wdat.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDONLY
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        random.seed(seed)
        randwalk = []
        while opcnt * blksize < fsize:
            randwalk.append(random.randint(0, fsize))
            opcnt += 1
        
        if self.verbosity >= VERBOSE_OP and \
           self.verbosity < VERBOSE_OP_DETAILS:
            self.verbose("randread: os.lseek->os.read(%s, %d) * %d" %
                    (file, blksize, fsize/blksize))
        
        if self.verbosity >= VERBOSE_OP_DETAILS:
            for offset in randwalk:
                self.verbose("randread: os.lseek(%s, %d, os.SEEK_SET)" %
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
            # os.SEEK_SET = 0, back compatibility
            # os.lseek(fd, offset, os.SEEK_SET)
            os.lseek(fd, offset, 0)
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
    
    def randwrite(self, file=None, flags=None, fsize=None, blksize=None,
                  seed=None):
        """randwrite""" # special purpose, do not modify
        minop = INTEGER_MAX
        maxop = INTEGER_MIN
        elapsed = opcnt = 0
        
        if file is None:
            file = self.wdat.file
        if flags is None:
            flags = os.O_CREAT | os.O_RDWR
        if fsize is None:
            fsize = self.fsize
        if blksize is None:
            blksize = self.blksize
    
        if self.syncio:
            flags = flags | os.O_SYNC
        
        random.seed(seed)
        randwalk = []
        while opcnt * blksize < fsize:
            randwalk.append(random.randint(0, fsize))
            opcnt += 1
        
        if self.verbosity >= VERBOSE_OP and \
           self.verbosity < VERBOSE_OP_DETAILS:
            self.verbose("randwrite: os.lseek->os.write(%s, %d) * %d" %
                    (file, blksize, fsize/blksize))
        
        if self.verbosity >= VERBOSE_OP_DETAILS:
            for offset in randwalk:
                self.verbose("randwrite: os.lseek(%s, %d, os.SEEK_SET)" %
                    (file, offset))
                self.verbose("randread: os.read(%s, %d) * %d" %
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
            # os.SEEK_SET = 0, back compatibility
            # os.lseek(fd, offset, os.SEEK_SET)
            os.lseek(fd, offset, 0)
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

#### Standalone routines ####
def parse_argv(argv):
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage,
                formatter=OptionParserHelpFormatter())
    
    parser.add_option("-w", "--wdir", action="store", type="string",
                dest="wdir", metavar="DIR", default=os.getcwd(),
                help="working directory (default: cwd)")
    
    parser.add_option('--mode', action='store', type='choice',
                dest='mode', metavar='auto/io/meta', default='auto',
                choices = ['auto', 'io', 'meta'],
                help="test mode\n"
                     "  auto: run both metadata and I/O test\n"
                     "    io: run I/O test only\n"
                     "  meta: run metadata test only\n")

    parser.add_option('-m', '--meta', action='store', type='string',
                dest='metaops', metavar='NUM,NUM', default='0',
                help="metadata operations to run (default: 0)\n"
                     "  0=all, 1=mkdir, 2=rmdir, 3=creat, 4=access,\n"
                     "  5=open, 6=open+close, 7=stat, \n"
                     "  8=stat_NONEXIST, 9=utime, 10=chmod, 11=unlink\n")
    
    parser.add_option('-i', '--io', action='store', type='string',
                dest='ioops', metavar='NUM,NUM', default='0',
                help="I/O operations to run (default: 0)\n"
                     " 0=all, 1=write, 2=rewrite, 3=read, 4=reread\n"
                     " 5=fwrite, 6=frewrite, 7=fread, 8=freread\n"
                     " 9=randwrite, 10=randread\n")
    
    parser.add_option("-s", "--fsize", action="store", type="string",
                dest="fsizerange", metavar="NUM,NUM", default="1MB",
                help="file size (default: 1MB)")
    
    parser.add_option("-b", "--blksize", action="store", type="string",
                dest="blksizerange", metavar="NUM,NUM", default="1KB",
                help="block size (default: 1KB)")
    
    parser.add_option("-c", "--count", action="store", type="string",
                dest="opcntrange", metavar="NUM,NUM", default='10',
                help="list of numbers of meta operations (default: 10)")
    
    parser.add_option("-f", "--factor", action="store", type="string",
                dest="factorrange", metavar="NUM", default='16',
                help="factor of directory tree (default: 16)") 
    
    parser.add_option("-v", "--verbosity", action="store", type="int",
                dest="verbosity", metavar="NUM", default=0,
                help="verbosity level: 0/1/2/3 (default: 0)")
    
    parser.add_option("-d", "--dryrun", action="store_true",
                dest="dryrun", default=False,
                help="dry run, do not execute (default: disabled)")
    
    parser.add_option("--without-open", action="store_false",
                dest="opentime", default=True,
                help="exclude open in timing (default: disable)")
    
    parser.add_option("--without-close", action="store_false",
                dest="closetime", default=True,
                help="exclude close in timing (default: disable)")
    
    parser.add_option("--syncio", action="store_true",
                dest="syncio", default=False,
                help="synchronized I/O (default: disabled)")
    
    parser.add_option("--fsync", action="store_true",
                dest="fsync", default=False,
                help="include fsync in write (default: disabled)")
    
    parser.add_option("--sleep", action="store", type="float",
                dest="sleep", metavar="SECONDS", default=0.0,
                help="sleep between operations (default: 0.0)")
    
    parser.add_option("--keep", action="store_true",
                      dest="keep", default=False,
                      help="keep temparary files (default: disabled)")
    
    parser.add_option("--report", action="store", type="string",
                      dest="reportdir", default=None,
                      help="report directory name (default: report-hhmmss)")
    
    parser.add_option("-u", "--unit", action="store", type="choice",
                dest="unit", metavar="KB/MB/GB", default="MB",
                choices = ['KB','MB','GB','kb','mb','gb',
                           'K','M','G','k','m','g'],
                help="unit of throughput in output (default: MB)")
    
    parser.add_option("--no-smartsize", action="store_false",
                dest="reportsmartsize", default=True,
                help="output human readable size in output (default: on)")
    
    opts, args = parser.parse_args(argv)
    
    # figure out the operations
    if opts.mode == 'auto' or opts.mode == 'io':
        opts.ioops = eval('[%s]' % opts.ioops)
        if 0 in opts.ioops:
            opts.ioops = list(OPSET_IO)
        else:
            # check op depency
            if 1 not in opts.ioops:
                opts.ioops.append(1)
            opts.ioops.sort()
            opts.ioops = map(lambda x:OPSET_IO[x-1], opts.ioops)
    else:
        opts.ioops = []
    
    if opts.mode == 'auto' or opts.mode == 'meta':
        opts.metaops = eval('[%s]' % opts.metaops)
        if 0 in opts.metaops:
            opts.metaops = list(OPSET_META)
        else:
            # check op depency
            if 2 in opts.metaops and 1 not in opts.metaops:
                opts.metaops.append(1)
            opts.metaops.sort()
            opts.metaops = map(lambda x:OPSET_META[x-1], opts.metaops)
    else:
        opts.metaops = []

    # figure out working directory
    opts.wdir = os.path.abspath(opts.wdir)
    opts.cmd = " ".join(sys.argv)

    # figure out file size and block size
    if opts.mode == 'auto' or opts.mode == 'io':
        opts.fsizerange = map(lambda x:parse_datasize(x), 
            opts.fsizerange.strip(',').split(','))
        opts.blksizerange = map(lambda x:parse_datasize(x), 
            opts.blksizerange.strip(',').split(','))
    if opts.mode == 'auto' or opts.mode == 'meta':
        opts.opcntrange = eval('[%s]' % opts.opcntrange)
        opts.factorrange = eval('[%s]' % opts.factorrange)
    
    opts.unit = opts.unit.upper()
    if not opts.unit.endswith('B'):
        opts.unit = opts.unit + 'B'
    return opts

def main():
    opts = parse_argv(sys.argv[1:])
    benchmark = BaseBenchmark(opts)
    benchmark.run()
    benchmark.report()
    return 0

if __name__ == "__main__":
    sys.exit(main())
