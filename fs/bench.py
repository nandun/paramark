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
from data import Database as FSDatabase

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
       
    def load(self):
        if self.cfg.gxpmode:
            self.gxp = Values()
            self.gxp.wp = os.fdopen(3, "wb")
            self.gxp.rp = os.fdopen(4, "rb")
            self.gxp.rank = gxp.get_rank()
            self.gxp.size = gxp.get_size()
            self.runtime.hid = self.gxp.rank
            self.runtime.nhosts = self.gxp.size
        
        self.threadsync = ThreadSync(self.cfg.nthreads)
        for i in range(0, self.cfg.nthreads):
            if i == 0 and self.cfg.gxpmode:
                cfg.gxp = self.gxp
            self.threads.append(BenchThread(i, self.threadsync, self.loader))

    def run(self):
        message("Start benchmarking ...")
        self.start = timer()
        
        for t in self.threads: t.start()
        for t in self.threads: t.join()

        if self.cfg.dryrun: message("Dryrun, nothing was executed.\n")
        
        self.end = timer()
        
        self.runtime.start = "%r" % self.start
        self.runtime.end = "%r" % self.end

    def save(self):
        if self.cfg.dryrun or self.cfg.quickreport: return
    
        if self.cfg.gxpmode:
            # Gather results
            self.send_res()
            if self.gxp.rank == 0:
                reslist = []
                for i in range(0, self.gxp.size):
                    reslist.append(self.recv_res())
            else:
                return
       
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
        if self.cfg.nolog:
            self.db = FSDatabase(":memory:")
        else:
            self.db = FSDatabase("%s/fsbench.db" % logdir)
        self.db.ins_runtime(self.runtime)
        self.db.ins_conf('%s/fsbench.conf' % logdir)

        if self.cfg.gxpmode:
            self.db.ins_rawdata(reslist.pop(0), self.start, True)
            for res in reslist:
                self.db.ins_rawdata(res, self.start)
        else:
            for t in self.threads:
                self.db.insert_rawdata(t.get_res())
        self.db.close()
        
        verbose("Saving benchmark data to %s/fsbench.db ..." % logdir, 
            VERBOSE)
    
    def report(self, path=None):
        if self.cfg.dryrun or self.cfg.noreport: return

        if self.cfg.quickreport:
            self.quick_report()
            return
        
        if self.cfg.gxpmode and self.gxp.rank != 0:
            return
        
        import report
        logdir = self.cfg.logdir
        if self.cfg.report:
            logdir = self.cfg.report
        if path:
            logdir = path
        if self.cfg.textreport:
            self.report = report.TextReport(logdir)
        else:
            self.report = report.HTMLReport(logdir)
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

    def quick_report(self):
        elapsed = {}
        for t in self.threads:
            res = t.get_res()
            _, sync_prev_time = res.synctime.pop(0)
            for sync_name, sync_time in res.synctime:
                if elapsed.has_key(sync_name):
                    elapsed[sync_name] += sync_time - sync_prev_time
                else:
                    elapsed[sync_name] = sync_time - sync_prev_time
                sync_prev_time = sync_time
        
        if self.cfg.gxpmode:
            # Gather results
            # self.send_res()
                #for k, v in elapsed:
                #    elapsed[k] = v / len(res.synctime)
            res = cPickle.dumps(elapsed, 0)
            self.gxp.wp.write('|'.join(res.split('\n')))
            self.gxp.wp.write("\n")
            self.gxp.wp.flush()
            if self.gxp.rank == 0:
                reslist = []
                for i in range(0, self.gxp.size):
                    reslist.append(self.recv_res())
            else:
                return
        else:
            reslist = [elapsed]
       
        io_opers = []
        io_aggs = []
        meta_opers = []
        meta_aggs = []
        elapsed = {}
        n_hosts = len(reslist)
        for res in reslist:
            for sync_name, sync_time in res.items():
                if elapsed.has_key(sync_name):
                    elapsed[sync_name] += sync_time
                else:
                    elapsed[sync_name] = sync_time
       
        for k in elapsed.keys():
            sync_time = elapsed[k] / (self.cfg.nthreads * n_hosts)
            if k in OPS_META:
                meta_opers.append(k)
                meta_aggs.append(self.cfg.opcnt * self.cfg.nthreads 
                    * n_hosts / sync_time)
            elif k in oper.OPS_IO:
                io_opers.append(k)
                io_aggs.append(self.cfg.fsize * self.cfg.nthreads * n_hosts
                    / sync_time)
        
        # Write report
        if self.cfg.logdir is None:
            self.cfg.logdir = os.path.abspath("./pmlog-%s-%s" %
                   (self.runtime.user, time.strftime("%j-%H-%M-%S")))
        
        if self.cfg.gxpmode:
            self.cfg.confirm = False
        self.cfg.logdir = smart_makedirs(self.cfg.logdir,
            self.cfg.confirm)
        
        f = _open("%s/report.txt" % self.cfg.logdir, "w")
        f.write("ParaMark: v%s, %s\n" 
            % (self.runtime.version, self.runtime.date))
        f.write("Platform: %s\n" % self.runtime.platform)
        f.write("Target: %s (%s)\n" % (self.runtime.wdir,
            self.runtime.mountpoint))
        f.write("Time: %s --- %s (%.2f seconds)\n" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(self.runtime.start)))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(self.runtime.end)))),
              (eval(self.runtime.end) - eval(self.runtime.start))))
        f.write("User: %s (%s)\n" % (self.runtime.user, self.runtime.uid))
        f.write("Command: %s\n" % self.runtime.cmdline)

        meta_aggs = map(lambda x:"%.3f"%x, meta_aggs)
        io_aggs = map(lambda x:"%.3f"%(x/1048576), io_aggs)

        if len(meta_aggs) > 0:
            f.write("\nMetadata Performance (ops/sec)\n")
            f.write("Oper: " + ",".join(meta_opers) + "\n")
            f.write("Aggs: " + ",".join(meta_aggs) + "\n")
        if len(io_aggs) > 0:
            f.write("\nI/O Performance (MB/sec)\n")
            f.write("Oper: " + ",".join(io_opers) + "\n")
            f.write("Aggs: " + ",".join(io_aggs) + "\n")
        
        f.close() 
        
class ThreadSync:
    def __init__(self, nthreads):
        self.n = nthreads
        self.cnt = 0
        self.lock = threading.Lock()
        self.event = threading.Event()

    def barrier(self):
        self.lock.acquire()
        if self.event.isSet():
            self.event.clear()
        self.cnt += 1
        if self.cnt == self.n:
            self.cnt = 0
            self.event.set()
        self.lock.release()
        self.event.wait()

class BenchThread(threading.Thread):
    def __init__(self, tid, sync, loader):
        threading.Thread.__init__(self)
        
        self.tid = tid
        self.sync = sync
        self.gxpmode = loader.cfg.gxpmode
        self.dryrun = loader.cfg.dryrun
        self.hid = loader.cfg.hid
        self.pid = loader.cfg.pid

        self.name = "Thread h%s:p%s:t%s" % (self.hid, self.pid, self.tid)
        self.wdir, self.load = loader.generate(self.tid)

        self.synctime = []

    def run(self):
        if not self.dryrun: os.makedirs(self.wdir)
        self.barrier()
        
        for op in self.load:
            op.exe()
            self.barrier(op.name)
        
        if not self.dryrun: shutil.rmtree(self.wdir)

    def barrier(self, name=""):
        self.sync.barrier()
        if self.gxpmode and self.tid == 0:
            self.gxp_barrier()
        self.synctime.append((name, timer()))

    def gxp_barrier(self):
        self.cfg.gxp.wp.write('\n')
        self.cfg.gxp.wp.flush()
        for i in range(self.cfg.gxp.size):
            r = self.cfg.gxp.rp.readline()
            if r == "": return 1
        return 0

    def get_res(self):
        val = Values()
        val.hid = self.hid
        val.pid = self.pid
        val.tid = self.tid
        val.synctime = self.synctime
        val.opset = [o.get() for o in self.load]
        return val
