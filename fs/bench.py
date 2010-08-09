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

# fs/bench.py
# File system benchmark

import os
import sys
import stat
import random
import shutil
import socket
import copy
import pwd
import cPickle
import StringIO
import threading

import version
from modules.verbose import *
from modules.common import *
from modules import num
from modules import gxp
from load import *
import oper
from data import Database as Database

VERBOSE = 1
VERBOSE_MORE = VERBOSE + 1
VERBOSE_ALL = VERBOSE_MORE + 1

__all__ = ['Bench']

class Bench:
    def __init__(self, opts):
        self.opts = opts
        self.cfg = self.opts.vals
        
        self.runtime = Values()
        self.runtime.version = version.PARAMARK_VERSION
        self.runtime.date = version.PARAMARK_DATE
        self.runtime.uid = os.getuid()
        self.runtime.pid = os.getpid()
        self.runtime.user = pwd.getpwuid(os.getuid())[0]
        self.runtime.hostname = socket.gethostname()
        self.runtime.platform = " ".join(os.uname())
        self.runtime.cmdline = " ".join(sys.argv)
        self.runtime.mountpoint = get_filesystem_info(self.cfg.wdir)
        self.runtime.wdir = self.cfg.wdir
        # May be set later in GXP mode
        self.runtime.hid = 0
        self.runtime.nhosts = 1

        self.cfg.hid = self.runtime.hid
        self.cfg.pid = self.runtime.pid
        self.loader = BenchLoad(self.cfg)
        self.threads = []
        self.db = None
        self.gxp = None
       
    def load(self):
        if self.cfg.gxpmode:
            self.gxp = Values()
            self.gxp.wp = os.fdopen(3, "wb")
            self.gxp.rp = os.fdopen(4, "rb")
            self.gxp.rank = gxp.get_rank()
            self.gxp.size = gxp.get_size()
            self.runtime.hid = self.gxp.rank
            self.runtime.nhosts = self.gxp.size
            self.cfg.hid = self.runtime.hid
        
        self.threadsync = ThreadSync(self.cfg.nthreads)
        for i in range(0, self.cfg.nthreads):
            self.threads.append(BenchThread(i, self.threadsync, 
                self.loader, self.gxp))

    def run(self):
        if self.runtime.hid == 0:
            message("Start benchmarking ...")
        
        self.start = timer()
        for t in self.threads: t.start()
        for t in self.threads: t.join()
        self.end = timer()

        if self.cfg.dryrun and self.runtime.hid == 0: 
            message("Dryrun, nothing was executed.\n")
        
        self.runtime.start = "%r" % self.start
        self.runtime.end = "%r" % self.end

    def save(self):
        if self.cfg.dryrun: return
    
        if self.cfg.gxpmode:
            # Gather results
            self.send_res()
            if self.gxp.rank == 0:
                reslist = []
                for i in range(0, self.gxp.size):
                    reslist.append(self.recv_res())
            else: return
       
        if self.cfg.logdir is None:  # generate random logdir in cwd
            self.cfg.logdir = os.path.abspath("./pmlog-%s-%s" %
                   (self.runtime.user, time.strftime("%j-%H-%M-%S")))
        
        # Initial log directory and database
        if self.cfg.gxpmode:
            self.cfg.confirm = False
        self.cfg.logdir = smart_makedirs(self.cfg.logdir,
            self.cfg.confirm)
        logdir = os.path.abspath(self.cfg.logdir)
        
        # Save used configuration file
        verbose("Saving configurations to %s/fsbench.conf ..." % logdir,
            VERBOSE)
        self.opts.save_conf("%s/fsbench.conf" % logdir)
        
        # Save results
        if self.cfg.nolog: self.db = Database(":memory:")
        else: self.db = Database("%s/fsbench.db" % logdir)
        self.db.insert_runtime(self.runtime)
        self.db.insert_conf(self.opts.cfgParser)

        if self.cfg.gxpmode:
            for res in reslist:
                for r in res: self.db.insert_rawdata(r)
        else:
            for t in self.threads:
                self.db.insert_rawdata(t.get_res())
        
        if self.cfg.nolog:
            verbose("Saving benchmark data in memory ...", VERBOSE)
        else:
            verbose("Saving benchmark data to %s/fsbench.db ..." % logdir, 
                VERBOSE)

        self.db.commit() 
        if self.cfg.noreport: self.db.close()
    
    def report(self):
        if self.cfg.dryrun or self.cfg.noreport: return

        if self.cfg.gxpmode and self.gxp.rank != 0: return
        
        import report
        logdir = self.cfg.logdir
        if self.cfg.report:
            logdir = self.cfg.report
        if self.cfg.textreport:
            self.report = report.TextReport(logdir, self.db, self.cfg)
        else:
            self.report = report.HTMLReport(logdir, self.db, self.cfg)
        self.report.write()
         
    def send_res(self):
        # Packing string without newlines
        res = cPickle.dumps([t.get_res() for t in self.threads], 0)
        self.gxp.wp.write('|'.join(res.split('\n')))
        self.gxp.wp.write("\n")
        self.gxp.wp.flush()

    def recv_res(self):
        res = self.gxp.rp.readline().strip('\n')
        return cPickle.loads('\n'.join(res.split('|')))
    
    def vs(self, msg):
        sys.stderr.write(msg)

class ThreadSync:
    def __init__(self, nthreads):
        self.n = nthreads
        self.cnt = 0
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.cv = threading.Condition(self.lock)
        self.barrier = self.barrier_condition
    
    def barrier_condition(self):
        """
        Barrier using condition variable
        """
        self.cv.acquire()
        self.cnt += 1
        if self.cnt == self.n:
            self.cnt = 0
            self.cv.notifyAll()
        else:
            self.cv.wait()
        self.cv.release()

    def barrier_event(self):
        """
        Barrier using event
        """
        self.lock.acquire()
        if self.event.isSet():
            self.event.clear()
        self.cnt += 1
        if self.cnt == self.n:
            self.cnt = 0
            self.lock.release()
            self.event.set()
        else:
            self.lock.release()
            self.event.wait()

class BenchThread(threading.Thread):
    def __init__(self, tid, sync, loader, gxp=None):
        threading.Thread.__init__(self)
        
        self.tid = tid
        self.sync = sync
        self.gxpmode = loader.cfg.gxpmode
        self.dryrun = loader.cfg.dryrun
        self.hid = loader.cfg.hid
        self.pid = loader.cfg.pid
        self.name = "Thread h%s:p%s:t%s" % (self.hid, self.pid, self.tid)
        self.wdir, self.load = loader.generate(self.tid)
        self.synctime = 0.0
        self.gxp = gxp

    def run(self):
        if not self.dryrun: os.makedirs(self.wdir)
        self.barrier()
        
        for op in self.load:
            op.exe()
            op.synctime = self.barrier()
        
        if not self.dryrun: shutil.rmtree(self.wdir)

    def barrier(self):
        self.sync.barrier()
        if self.gxpmode and self.tid == 0:
            self.gxp_barrier()
        previous = self.synctime
        self.synctime = timer()
        return self.synctime - previous

    def gxp_barrier(self):
        self.gxp.wp.write('\n')
        self.gxp.wp.flush()
        for i in range(self.gxp.size):
            r = self.gxp.rp.readline()
            if r == "": return 1
        return 0

    def get_res(self):
        val = Values()
        val.hid = self.hid
        val.pid = self.pid
        val.tid = self.tid
        val.opset = [o.get() for o in self.load]
        return val
