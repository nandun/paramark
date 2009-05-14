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

import csv
import errno
import fcntl
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
from basemark import *

# GXP stuff
def get_rank():
    return int(os.environ.get("GXP_EXEC_IDX", "0"))

def get_size():
    return int(os.environ.get("GXP_NUM_EXECS", "1"))

class Host:
    def __init__(self, h, f, i, idx):
        self.h = h      # hostname
        self.f = f      # fqdn
        self.i = i      # ip address
        self.idx = idx  # GXP_EXEC_IDX
        self.key = h    # misc usage

    def __repr__(self):
        return ("Host(%(h)r,%(f)r,%(i)r,%(idx)r)" % self.__dict__)

    def match_regexp(self, regexp):
        return regexp.match(self.f)

def get_my_host():
    h = socket.gethostname()
    f = socket.getfqdn()
    try:
        i = socket.gethostbyname(f)
    except socket.gaierror:
        es("warning: failed to get ip address of %s\n" % f)
        i = '127.0.0.1'
    idx = get_rank()
    return Host(h, f, i, idx)

def get_all_hosts(wp, fp):
    wp.write("%r\n" % get_my_host())
    wp.flush()
    hosts = []
    for i in range(get_size()):
        line = fp.readline()
        assert line != ""
        host = eval(line.strip())
        hosts.append((host.idx, host))
    hosts.sort()
    hosts_list = map(lambda (idx,host): host, hosts)
    hosts_map = {}
    for h in hosts_list:
        hosts_map[h.key] = h
    return hosts_list, hosts_map

def set_close_on_exec():
    try:
        fd_3 = fcntl.fcntl(3, fcntl.F_GETFD)
        fd_4 = fcntl.fcntl(4, fcntl.F_GETFD)
    except IOError:
        fd_3 = fcntl.FD_CLOEXEC
        fd_4 = fcntl.FD_CLOEXEC
    fd_3 = fd_3 | fcntl.FD_CLOEXEC
    fd_4 = fd_4 | fcntl.FD_CLOEXEC
    fcntl.fcntl(3, fcntl.F_SETFD, fd_3)
    fcntl.fcntl(4, fcntl.F_SETFD, fd_4)

class ParaDataGenerator(DataGenerator):
    def __init__(self, host, rank, size, wp, fp, threads, base):
        DataGenerator.__init__(self, base)
        self.rank = rank
        self.host = host
        self.size = size
        self.wp = wp
        self.fp = fp
        self.threads = 1
        self.local_files = None
        self.local_dirs = None
        self.global_files = None  # [ file1, file2, ..., filen ]
        self.global_dirs = None
        self.files_set = None     # set[rank] = [ file1, file2, ... ]
        self.dirs_list = None
        
        # internal variable
        self.round_cnt = 0
    
    def broadcast(self, msg):
        self.wp.write(msg)
        self.wp.write('\n') # Why?
        self.wp.flush()
    
    def receive(self):
        msg = self.fp.readline()
        assert msg != ""
        return msg.strip()
    
    def gendirs(self, num, factor=16):
        DataGenerator.gendirs(self, num, factor)
        self.local_dirs = list(self.dirs)
    
    def genfiles(self, num):
        DataGenerator.genfiles(self, num)
        self.local_files = list(self.files)
    
    def mergedirs(self):
        self.global_dirs = []
        self.dirs_set = {}
        self.broadcast(repr((self.rank, self.local_dirs)))
        for i in range(0, self.size):
            rank, dirs = eval(self.receive())
            self.dirs_set[rank] = dirs
            self.global_dirs.extend(dirs)
    
    def mergefiles(self):
        self.global_files = []
        self.files_set = {}
        self.broadcast(repr((self.rank, self.local_files)))
        for i in range(0, self.size):
            rank, files = eval(self.receive())
            self.files_set[rank] = files
            self.global_files.extend(files)

    def shuffle(self, shuffle="random", round=1):
        if shuffle == "random":
            if self.global_dirs is not None:
                if self.rank == 0:
                    random.shuffle(self.dirs_set)
                    self.broadcast(repr(self.dirs_set))
                self.dirs_set = eval(self.receive())
                self.dirs = self.dirs_set[self.rank]
                self.dir = self.dirs[0]
            if self.global_files is not None:
                if self.rank == 0:
                    random.shuffle(self.files_set)
                    self.broadcast(repr(self.files_set))
                self.files_set = eval(self.receive())
                self.files = self.files_set[self.rank]
                self.file = self.files[0]
        elif shuffle == "round":
            self.round_cnt += 1
            if self.global_dirs is not None:
                self.dirs = self.dirs_set[(self.rank + round * 
                    self.round_cnt) % self.size]
                self.dir = self.dirs[0]
            if self.global_files is not None:
                self.files = self.files_set[(self.rank + round * 
                    self.round_cnt) % self.size]
                self.file = self.files[0]

class ParaMark(BaseBenchmark):
    def __init__(self, hosts, rank, wp, fp, opts=None, **kw):
        BaseBenchmark.__init__(self, opts, **kw)

        # GXP runtime variables
        self.rank = rank
        self.host = hosts[rank]
        self.size = get_size()
        self.wp = wp
        self.fp = fp

        self.wdat = ParaDataGenerator(self.host, rank, self.size, wp, fp,
            1, self.wdir)
        
        self.res = []

    def barrier(self):
        self.wp.write('\n')
        self.wp.flush()
        for i in range(self.size):
            r = self.fp.readline()
            if r == "":
                return -1
        return 0
    
    def broadcast(self, msg):
        self.wp.write(msg)
        self.wp.write('\n') # Why?
        self.wp.flush()
    
    def receive(self):
        msg = self.fp.readline()
        assert msg != ""
        return msg.strip()
    
    def preprocess(self, test=None):
        """override BaseBenchmark:preprocess"""
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
            self.wdat.mergefiles()
            return map(lambda x:self.opdict[x], self.ioops)
        if self.optype == 'meta':
            if self.verbosity >= VERBOSE_FILE_DIR:
                self.verbose("preprocess: self.wdat.gendirs()")
                self.verbose("preprocess: self.wdat.genfiles()")
            self.wdat.gendirs(self.opcnt, self.factor)
            self.wdat.genfiles(self.opcnt)
            self.wdat.mergedirs()
            self.wdat.mergefiles()
            return map(lambda x:self.opdict[x], self.metaops)
    
    def interprocess(self, op, ops):
        """override BaseBenchmark:interprocess"""
        if self.verbosity >= VERBOSE_CHECKPOINT:
            self.verbose("overrided interprocess:\n")
        
        if self.shuffle:
            if ops.index(op) + 1 < len(ops):
                nextop = ops[ops.index(op) + 1].__doc__
            else:
                nextop = "None"
            if nextop in ["rewrite", "reread", "frewrite", "freread"]:
                if self.verbosity >=3:
                    self.verbose("interprocess: self.wdat.shuffle(%s, %d)" 
                                 % (self.shuffle, self.round))
                self.wdat.shuffle(self.shuffle, self.round)

        if self.verbosity >= 3:
            self.verbose("interprocess: time.sleep(%f)" % self.sleep)
        if not self.dryrun and self.sleep > 0:
            time.sleep(self.sleep)
    
    def postprocess(self, test):
        # cleanup
        if self.verbosity >= 3:
            self.verbose("postprocess: self.wdat.clean()")
        if not self.dryrun and not self.keep:
            start = timer()
            self.wdat.clean()
            test.env['cleanup_time'] =  timer() - start
        else:
            test.env['cleanup_time'] = 0

        # send and aggregate results
        self.broadcast(repr(test.dat))
        if test.env["optype"] == "io":
            self.res.append(self.aggoutput_io(test))
        elif test.env["optype"] == "meta":
            self.res.append(self.aggoutput_meta(test))

    def aggoutput_io(self, test):
        # must be done to clean receive buffer
        buffer = [eval(self.receive()) for i in range(0, self.size)]
        if self.rank != 0:
            return None
        res = TestSet()
        res.id = test.id
        res.env.update(test.env)
        fsize = test.env["fsize"]
        for i in range(0, len(test.dat)):
            minthput = ""
            maxthput = -1
            sumthput = 0
            aggthput = 0
            allstart = ""
            allend = -1
            for b in buffer:
                opname, opcnt, opmin, opmax, elapsed, start, end = b[i]
                thput = fsize/elapsed
                minthput = min(minthput, thput)
                maxthput = max(maxthput, thput)
                sumthput += thput
                allstart = min(allstart, start)
                allend = max(allend, end)
            #aggthput = fsize * self.size / (allend - allstart)
            aggthput = fsize * self.size / test.env["%s_elapsed" % opname]
            res.dat.append((opname, minthput, maxthput, sumthput, aggthput))
        return res
                
    def aggoutput_meta(self, test):
        buffer = [eval(self.receive()) for i in range(0, self.size)]
        if self.rank != 0:
            return None
        res = TestSet()
        res.id = test.id
        res.env.update(test.env)
        for i in range(0, len(test.dat)):
            minthput = ""
            maxthput = -1
            sumthput = 0
            aggthput = 0
            allstart = ""
            allend = -1
            for b in buffer:
                opname, opcnt, opmin, opmax, elapsed, start, end = b[i]
                thput = opcnt/elapsed
                minthput = min(minthput, thput)
                maxthput = max(maxthput, thput)
                sumthput += thput
                allstart = min(allstart, start)
                allend = max(allend, end)
            #aggthput = opcnt * self.size / (allend - allstart)
            aggthput = opcnt * self.size / test.env["%s_elapsed" % opname]
            res.dat.append((opname, minthput, maxthput, sumthput, aggthput))
        return res
    
    def checktestset(self, test):
        if self.rank == 0:
            self.broadcast("%d" % test.id)
        testid = eval(self.receive())
        if test.id != testid:
            ws("[%9s#%5d]error: inconsistent test set\n" % 
                (self.hostname, self.pid))
            sys.exit(1)

    def run(self):
        """override BaseBenchmark:run"""
        self.start = (time.localtime(), timer())
        for test in self.tests:
            # check if everyone performs the same test set
            self.checktestset(test)
            ops = self.preprocess(test)
            for op in ops:
                self.barrier()
                elapsed = timer()       # all finish time
                res = op()
                self.barrier()
                elapsed = timer() - elapsed
                test.dat.append(res)
                test.env["%s_elapsed" % op.__doc__] = elapsed
                self.interprocess(op, ops)
            self.postprocess(test)
        self.end = (time.localtime(), timer())
    
    ### reporing routines ###
    def reporttocsv(self, tests=None, reportdir=None):
        """aggregate result and store to csv file"""
        if tests is None:
            tests = self.res
        if reportdir is None:
            reportdir = self.reportdir
        
        iof = open("%s/io.csv" % reportdir, "wb")
        metaf = open("%s/meta.csv" % reportdir, "wb")
        iocsv = csv.writer(iof)
        metacsv = csv.writer(metaf)
        
        # write csv header
        iocsv.writerow(["operation", "proc#", "filesize", "blocksize",
            "min/proc", "max/proc", "summation", "aggregation"])
        metacsv.writerow(["operation", "proc#", "count", "factor", "min/proc",
            "max/proc", "summation", "aggregation"])
    
        unit = eval(self.unit)
        for t in tests:
            if t.env["optype"] == "meta":
                for d in t.dat:
                    #nproc = t.env["nproc"]
                    nproc = self.size
                    opcnt = t.env["opcnt"]
                    factor = t.env["factor"]
                    opname, minthput, maxthput, sumthput, aggthput = d
                    row = [opname, nproc, opcnt, factor, minthput, maxthput,
                        sumthput, aggthput]
                    metacsv.writerow(row)
            elif t.env["optype"] == "io":
                for d in t.dat:
                    #nproc = t.env["nproc"]
                    nproc = self.size
                    fsize = t.env["fsize"]
                    blksize = t.env["blksize"]
                    opname, minthput, maxthput, sumthput, aggthput = d
                    row = [opname, nproc,
                        "%s%s" % smart_datasize(fsize),
                        "%s%s" % smart_datasize(blksize),
                        "%s" % (minthput/unit), "%s" % (maxthput/unit),
                        "%s" % (sumthput/unit), "%s" % (aggthput/unit)]
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
ParaMark (version %s, %s)
          platform: %s
         run began: %s
           run end: %s
          duration: %s seconds
              user: %s (%s)
           command: %s
 working directory: %s
         Processes: %s
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
           self.size,
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
        
        if self.rank == 0:
            self.reportstatus()
            self.reporttocsv(self.res)

#
# main entry
#
def parse_argv(argv):
    usage = "gxpc mw %prog [options]"
    
    parser = optparse.OptionParser(usage=usage,
                formatter=OptionParserHelpFormatter())
    
    parser.remove_option("-h")
    parser.add_option("-h", "--help", action="store_true",
                      dest="help", default=False,
                      help="show the help message and exit")
    
    # control options, keep consistent with basemark's variables
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
    
    parser.add_option("-u", "--unit", action="store", type="choice",
                dest="unit", metavar="KB/MB/GB", default="MB",
                choices = ['KB','MB','GB','kb','mb','gb',
                           'K','M','G','k','m','g'],
                help="unit of throughput (default: B)")
    
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
    
    parser.add_option("--shuffle", action="store", type="string",
                dest="shuffle", default=None,
                help="shuffle: random/round (default: disabled)")
    
    parser.add_option("--round", action="store", type="int",
                dest="round", default=1,
                help="offset in round shuffle (default: 1)")
    
    parser.add_option("--sleep", action="store", type="float",
                dest="sleep", metavar="SECONDS", default=0.0,
                help="sleep between operations (default: 0.0)")
    
    parser.add_option("--keep", action="store_true",
                      dest="keep", default=False,
                      help="keep temparary files (default: disabled)")
    
    parser.add_option("--report", action="store", type="string",
                      dest="reportdir", default=None,
                      help="report directory name (default: report-hhmmss)")

    opts, args = parser.parse_args(argv)
    
    opts.print_help = parser.print_help

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
    
    return opts, None

def main():
    # initial GXP-related environments
    try:
        set_close_on_exec()
    except IOError:
        opts, errstr = parse_argv([])
        opts.print_help()
        return 1
    wp = os.fdopen(3, "wb")
    fp = os.fdopen(4, "rb")
    hosts, hosts_map = get_all_hosts(wp, fp)
    if hosts is None:
        es("error: failed to get all hosts\n")
        return 1
    rank = get_rank()
    myhost = hosts[rank]
    
    # parsing arguments
    opts, errstr = parse_argv(sys.argv[1:])
    if opts is None:
        if rank == 0:
            ws(errstr)
        return 1
    if opts.help:
        if rank == 0:
            opts.print_help();
        return 0
    
    # execution
    pm = ParaMark(hosts, rank, wp, fp, opts)
    pm.run()
    pm.report()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
