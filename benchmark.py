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
# Benchmarks
#

import sys
import os
import pwd
import socket
import shutil
import time

from common import *
from fsop import FileSystemOperation
from database import SerialBenchmarkDB

__all__ = ["Benchmark", "SerialBenchmark", "ParallelBenchmark"]

class TestSet:
    def __init__(self):
        self.id = None  # hash value of following arguments: env and ops
        self.env = {}   # configuration of current test
        self.ops = []   # list of operations to perform
        self.dat = []   # raw output from oplist
    
class Benchmark:
    """
    Benchmark base class, which includes common runtime variables
    and utilties.
    """
    def __init__(self, opts=None, **kw):
        # configurable variables
        self.mode = None
        self.nproc = 1
        self.fsizerange = None
        self.blksizerange = None
        self.opcntrange = None
        self.factorrange = None
        self.metaops = None
        self.ioops = None
        
        self.shuffle = None 
        self.round = 1
        
        self.sleep = 0.0
        self.keep = False
        
        self.reportdir = None
        self.reportsmartsize = None
        
        self.dryrun = False
        self.verbosity = 0
    
        _Benchmark_restrict = ["mode", "nproc", "fsizerange", 
            "blksizerange", "opcntrange", "factorrange", 
            "metaops", "ioops", "shuffle",
            "round", "sleep", "keep", "reportdir", "reportsmartsize", 
            "dryrun", "verbosity"]
        update_opts_kw(self.__dict__, _Benchmark_restrict, 
            opts, kw)
        
        # 
        # Following variables will not updated by opts or **kw
        #
        
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        self.cmdline = " ".join(sys.argv)
        
        self.start = None
        self.end = None

        self.verbosecnt = 0

        self.ws = sys.stdout.write
        self.es = sys.stderr.write
        
        self.tests = []

        # Post initialization
        self.ensuredir(os.path.dirname(self.reportdir))

        if self.mode in ["auto", "io"]:
            self.tests.extend(self.gen_io_tests())
        if self.mode in ["auto", "meta"]:
            self.tests.extend(self.gen_meta_tests())
    
    def verbose(self, msg):
        self.ws("[%9s#%5d:%05d] %s\n" % 
            (self.hostname, self.pid, self.verbosecnt, msg))
        self.verbosecnt += 1

    def ensuredir(self, path):
        if os.path.isdir(path):
            return 0
        
        if self.verbosity >= VERBOSE_WARNING:
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
    
    def clean(self, tempdir=None):
        if tempdir is None:
            tempdir = self.tempdir
        shutil.rmtree(tempdir)

    def gen_io_tests(self, nproc=None, fsizerange=None, 
        blksizerange=None, ioops=None):
        if nproc is None:
            nproc = self.nproc
        if fsizerange is None:
            fsizerange = self.fsizerange
        if blksizerange is None:
            blksizerange = self.blksizerange
        if ioops is None:
            ioops = self.ioops
            
        tests = []
        for fsize in fsizerange:
            for blksize in blksizerange:
                t = TestSet()
                t.env["optype"] = "io"
                t.env["nproc"] = nproc
                t.env["fsize"] = fsize
                t.env["blksize"] = blksize
                t.ops = ioops
                t.id = string_hash("%r%r" % (t.env, t.ops))
                tests.append(t)
        return tests
    
    def gen_meta_tests(self, nproc=None, opcntrange=None, 
        factorrange=None, metaops=None):
        if nproc is None:
            nproc = self.nproc
        if opcntrange is None:
            opcntrange = self.opcntrange
        if factorrange is None:
            factorrange = self.factorrange
        if metaops is None:
            metaops = self.metaops
            
        tests = []
        for opcnt in opcntrange:
            for factor in factorrange:
                t = TestSet()
                t.env["optype"] = "meta"
                t.env["nproc"] = nproc
                t.env["opcnt"] = opcnt
                t.env["factor"] = factor
                t.ops = metaops
                t.id = string_hash("%r%r" % (t.env, t.ops))
                tests.append(t)
        return tests
        
class SerialBenchmark(Benchmark, FileSystemOperation, SerialBenchmarkDB):
    def __init__(self, opts=None, **kw):
        Benchmark.__init__(self, opts, **kw)
        FileSystemOperation.__init__(self, opts, **kw)
        SerialBenchmarkDB.__init__(self, self.reportdir)

    def preprocess(self, test=None):
        if test is not None:
            self.__dict__.update(test.env)
        
        self.gen_tempdir("paramark-%s-%s" % (self.hostname, self.user));
        if self.verbosity >= VERBOSE_STAGE:
            self.verbose("preprocess: ensuredir(%s)" % self.tempdir)
        self.ensuredir(self.tempdir)
        
        if self.optype == 'io':
            if self.verbosity >= VERBOSE_INFO:
                self.verbose("preprocess: self.gen_files()")
            self.gen_files(self.nproc)
            return map(lambda x:self.op[x], self.ioops)
        elif self.optype == 'meta':
            if self.verbosity >= VERBOSE_INFO:
                self.verbose("preprocess: self.gen_dirs(%d, %d)" %
                    (self.opcnt, self.factor))
                self.verbose("preprocess: self.gen_files(%d)" % self.opcnt)
            self.gen_dirs(self.opcnt, self.factor)
            self.gen_files(self.opcnt)
            return map(lambda x:self.op[x], self.metaops)
    
    def interprocess(self):
        if self.verbosity >= VERBOSE_STAGE:
            self.verbose("interprocess:\n")

        if self.verbosity >= 3:
            self.verbose("interprocessing: time.sleep(%f)" % self.sleep)
        if not self.dryrun and self.sleep > 0:
            time.sleep(self.sleep)

    def postprocess(self):
        if self.verbosity >= VERBOSE_STAGE:
            self.verbose("postprocess: self.clean()")
        if not self.dryrun and not self.keep:
            start = timer()
            self.clean()
            return ('cleanup_time', timer() - start)
        else:
            return ('cleanup_time', 0)
    
    def run(self):
        self.start = (time.localtime(), timer())
        for test in self.tests:
            ops = self.preprocess(test)
            for op in ops:
                test.dat.append(op())
                self.interprocess()
            self.postprocess()
        self.end = (time.localtime(), timer())
        self.store()
    
    # Data persistence routines
    def store(self, tests=None):
        if tests is None:
            tests = self.tests
        self.create_tables()
        self.store_env()
        self.store_result()
        self.close_database()

        # prompt user
        if self.verbosity >= VERBOSE_PROMPT:
            self.ws(\
"""\
Benchmarking results have been written to %s.
Please use paramark or sqlite3 to view and process your data.
See paramark -h for more help.
""" % self.dbfile)
        
    def store_env(self):
        # environmental variables
        from version import PARAMARK_VERSION, PARAMARK_DATE
        self.env_insert("dbtype", "serialbenchmark")
        self.env_insert("ParaMark Version", PARAMARK_VERSION)
        self.env_insert("ParaMark Date", PARAMARK_DATE)
        self.env_insert("platform", self.platform)
        self.env_insert("run began", "%r" % self.start[0])
        self.env_insert("run end", "%r" % self.end[0])
        self.env_insert("duration", "%s" % (self.end[1] - self.start[1]))
        self.env_insert("user", self.user)
        self.env_insert("uid", "%s" % self.uid)
        self.env_insert("command", self.cmdline)
        self.env_insert("working directory", self.wdir)
        self.env_insert("mode", self.mode)
        self.commit_data()
    
    def store_result(self, tests=None):
        if tests is None:
            tests = self.tests

        for t in tests:
            if t.env["optype"] == "meta":
                for d in t.dat:
                    nproc = t.env["nproc"]
                    factor = t.env["factor"]
                    opname, opcnt, opmin, opmax, elapsed, start, end = d
                    row = (opname, nproc, opcnt, factor, elapsed, opmin, 
                        opmax, "%s" % (opcnt/elapsed))
                    self.meta_insert(row)
            elif t.env["optype"] == "io":
                for d in t.dat:
                    nproc = t.env["nproc"]
                    fsize = t.env["fsize"]
                    blksize = t.env["blksize"]
                    opname, opcnt, opmin, opmax, elapsed, start, end = d
                    row = (opname, nproc, "%s" % fsize, "%s" % blksize, 
                        elapsed, opmin, opmax, "%s" % (fsize/elapsed))
                    self.io_insert(row)

        self.commit_data()
        
class ParallelBenchmark:
    def __init__(self):
        return
