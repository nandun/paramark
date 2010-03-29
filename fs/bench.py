#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
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
import threading
from __builtin__ import open as _open # for open()

import version
import modules.utils as utils

OPTYPE_META = 1
OPTYPE_IO = 0

__all__ = ["OPTYPE_META", "OPTYPE_IO"]

class Bench():
    """
    General file system benchmark
    """
    def __init__(self, opts):
        self.opts = opts
        self.cfg = self.opts.opts
        
        # Benchmark runtime environment
        self.runtime = self.opts.new_values()
        self.runtime.version = version.PARAMARK_VERSION
        self.runtime.date = version.PARAMARK_DATE
        self.runtime.uid = os.getuid()
        self.runtime.pid = os.getpid()
        self.runtime.user = pwd.getpwuid(os.getuid())[0]
        self.runtime.hostname = socket.gethostname()
        self.runtime.platform = " ".join(os.uname())
        self.runtime.cmdline = " ".join(sys.argv)
        self.runtime.environ = copy.deepcopy(os.environ)
        self.runtime.mountpoint = utils.get_fs_info(self.cfg.wdir)
        self.runtime.wdir = self.cfg.wdir
        
        self.threads = []

        self.VERBOSE_LEVEL = 3

    def vs(self, msg):
        sys.stderr.write(msg)

    def report(self, path=None):
        logdir = self.opts["logdir"]
        if self.opts["report"]:
            logdir = self.opts["report"]
        if path:
            logdir = path
        self.report = fs.report.HTMLReport(self.opts["logdir"])
        self.report.write()
         
    def load(self):
        # TODO: setup GXP id here
        self.cfg.hostid = 0
        self.cfg.pid = os.getpid()
        
        self.thread_sync = ThreadSync(self.cfg.nthreads)
        for i in range(0, self.cfg.nthreads):
            self.cfg.tid = i
            self.threads.append(BenchThread(self.cfg, self.thread_sync))

    def run(self):
        self.start = utils.timer()
        
        # shuffle threads to avoid scheduling effect
        #random.shuffle(self.threads)
        for t in self.threads:
            t.start()

        for t in self.threads:
            t.join()
        
        if self.opts["dryrun"]:
            sys.stdout.write("Dryrun, nothing executed.\n")
        
        self.end = utils.timer()
        
        self.runtime["start"] = "%r" % self.start
        self.runtime["end"] = "%r" % self.end

    def save(self):
        if self.opts["dryrun"]:
            return
        
        if self.cfg.logdir is None:  # generate random logdir in cwd
            self.cfg.logdir = os.path.abspath("./pmlog-%s-%s" %
                (self.runtime.user, time.strftime("%j-%H-%M-%S")))
        
        # Initial log directory and database
        self.opts["logdir"] = utils.smart_makedirs(self.opts["logdir"],
            self.opts["confirm"])
        logdir = os.path.abspath(self.opts["logdir"])
        
        # Save used configuration file
        self.config.save_conf("%s/fsbench.conf" % logdir)
        if self.opts["verbosity"] >= 1:
            self.vs("applied configurations saved to %s/fsbench.conf\n" 
                % logdir)
        
        # Save results
        self.db = fs.data.Database("%s/fsbench.db" % logdir, True)
        self.db.ins_runtime(self.runtime)
        self.db.ins_conf('%s/fsbench.conf' % logdir)
        self.db.ins_rawdata(self.threads, self.start)
        self.db.close()

        if self.opts["verbosity"] >= 1:
            self.vs("raw benchmark data saved to %s/fsbench.db\n" % logdir)

class ThreadSync():
    """Thread synchornization"""
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
    """Benchmarking thread that executes a series of operations"""
    def __init__(self, opts, sync):
        threading.Thread.__init__(self)
        
        self.opts = opts
        self.sync = sync
        self.synctime = []
        self.hostid = opts["hostid"]
        self.pid = opts["pid"]
        self.tid = opts["tid"]
        self.verbose = opts["verbose"]
        self.dryrun = opts["dryrun"]
        self.ioopts = opts["ioopts"]
        self.name = "Thread h%s:p%s:t%s" % (self.hostid, self.pid, self.tid)

        self.load = BenchLoad(opts)
        self.load.produce()

        self.opset = []
        for o in opts["metaops"] + opts["ioops"]:
            ostr = "%s(files=self.load.%s(), " \
                "verbose=%s, dryrun=%s, **self.opts[\"%s\"])" \
                % (o, o, self.verbose, self.dryrun, o)
            op = eval(ostr)
            self.opset.append(op)

    def run(self):
        if not self.dryrun:
            os.makedirs(self.load.root_dir())
        self.barrier()
        
        for op in self.opset:
            op.execute()
            self.barrier(op.name)
        
        if not self.dryrun:
            shutil.rmtree(self.load.root_dir())

    def barrier(self, name=""):
        self.sync.barrier()
        self.synctime.append((name, timer()))
        #if len(self.synctime) > 1:
        #    print name, self.synctime[-1][1] - self.synctime[-2][1]
        
__all__.append("BenchThread")

class BenchLoad:
    """Metadata workload generator"""
    def __init__(self, opts, **kw):
        for o in ["wdir", "hostid", "pid", "tid"]: 
            self.__dict__[o] = opts[o]
        
        # only retrieve metadata part options
        self.opts = {}
        for o in FSOP_META + FSOP_IO:
            self.opts[o] = opts[o]
        
        for k, v in opts["metaopts"].items(): 
            self.__dict__[k] = v

        self.rdir = None    # root directory to perform load
        self.dirs = []
        self.dir = None
        self.files = []
        self.file = None

        for o in ["creat", "access", "open", "open_close", "stat_exist",
            "stat_non", "utime", "chmod", "rename", "unlink"]:
            self.__dict__[o] = self.get_files

        for o in FSOP_IO:
            self.__dict__[o] = self.get_file
    
    def produce(self):
        self.rdir = "%s/pmark-wdir-%s-%s-%s-%03d" \
            % (self.wdir, self.hostid, self.pid, self.tid, 
            random.randint(0,999))
        
        queue = [ copy.deepcopy(self.rdir) ]
        i = l = 0
        while i < self.opcnt:
            if i % self.factor == 0:
                parent = queue.pop(0)
                l += 1
            child = os.path.normpath("%s/L%d-%d" % (parent, l, i))
            self.dirs.append(child)
            self.files.append("%s/%d-%d.file" % (child, l, i))
            queue.append(child)
            i += 1

        self.file = "%s/io-%d.file" % (self.rdir, random.randint(0,999))

    def root_dir(self):
        return self.rdir

    def get_dirs(self):
        return self.dirs
    
    def get_files(self):
        return self.files

    def get_file(self):
        return self.file

    def mkdir(self):
        # TODO: setup on local options
        return self.dirs

    def rmdir(self):
        # TODO: setup on local options
        dirs = list(self.dirs)
        dirs.reverse()
        return dirs

class Op:
    def __init__(self):
        self.name = None
        self.type = None
        self.files = None
        self.verbose = None
        self.dryrun = None
        self.res = []

    def updatekw(self, kw):
        if kw is not None:
            for k, v in kw.items():
                if self.__dict__.has_key(k): self.__dict__[k] = v

    def vs(self, msg):
        sys.stderr.write("%s\n" % msg)
    
    def execute(self):
        sys.stderr.write("nometaop\n")
        return None

class MetaOp(Op):
    """Metadata operation base class"""
    def __init__(self, name, files, verbose=False, dryrun=False):
        Op.__init__(self)
        self.type = OPTYPE_META
        self.name = name
        self.files = files
        self.opcnt = len(files)
        self.verbose = verbose
        self.dryrun = dryrun
    
FSOP_META = ["mkdir", "creat", "access", "open", "open_close", "stat_exist", 
    "stat_non", "utime", "chmod", "rename", "unlink", "rmdir"]
__all__.extend(FSOP_META)

class mkdir(MetaOp):
    """Make a list of directories by os.mkdir()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "mkdir", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose:
                self.vs("mkdir: os.mkdir(%s)" % f)
            if self.dryrun:
                continue
            s = timer()
            os.mkdir(f)
            self.res.append((s, timer()))
        
        if not self.dryrun:
            assert len(self.res) == len(self.files)
        return self.res
        
class rmdir(MetaOp):
    """Remove a list of directories by os.rmdir()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "rmdir", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose: self.vs("os.rmdir(%s)" % f)
            if self.dryrun: continue
            s = timer()
            os.rmdir(f)
            self.res.append((s, timer()))
        
        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res

class creat(MetaOp):
    """Create a list of files by os.open() and os.close() pairs"""
    def __init__(self, files, flags=os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 
        mode=stat.S_IRUSR | stat.S_IWUSR, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "creat", files, verbose, dryrun)
        self.flags = flags
        self.mode = mode
        self.updatekw(kw)
    
    def execute(self):
        for f in self.files:
            if self.verbose:
                self.vs("os.close(os.open(%s, 0x%x, 0x%x))" 
                    % (f, self.flags, self.mode))
            if self.dryrun: continue
            s = timer()
            os.close(os.open(f, self.flags, self.mode))
            self.res.append((s, timer()))

        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res

class access(MetaOp):
    """Access a list of files by os.access()"""
    def __init__(self, files, mode=os.F_OK, verbose=False, dryrun=False, 
        **kw):
        MetaOp.__init__(self, "access", files, verbose, dryrun)
        self.mode = mode
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose:
                self.vs("os.access(%s, 0x%x)" % (f, self.mode))
            if self.dryrun: continue
            s = timer()
            os.access(f, self.mode)
            self.res.append((s, timer()))
        
        if not self.dryrun:
            assert len(self.res) == len(self.files)
        return self.res

class open(MetaOp): # shadows __builtin__.open
    """Open a list of files by os.open()"""
    def __init__(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False,
        **kw):
        MetaOp.__init__(self, "open", files, verbose, dryrun)
        self.flags = flags
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose:
                self.vs("os.open(%s, 0x%x)" % (f, self.flags))
            if self.dryrun: continue
            s = timer()
            fd = os.open(f, self.flags)
            self.res.append((s, timer()))
            os.close(fd)
        
        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res

class open_close(MetaOp):
    """Access a list of files by os.open() and os.close() pairs"""
    def __init__(self, files, flags=os.O_RDONLY, verbose=False, dryrun=False,
        **kw):
        MetaOp.__init__(self, "open_close", files, verbose, dryrun)
        self.flags = flags
        self.updatekw(kw)
    
    def execute(self):
        for f in self.files:
            if self.verbose:
                self.vs("os.close(os.open(%s, 0x%x))" % (f, self.flags))
            if self.dryrun: continue
            s = timer()
            os.close(os.open(f, self.flags))
            self.res.append((s, timer()))
        
        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res

class stat_exist(MetaOp):
    """Access a list of files by os.stat()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "stat_exist", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose: self.vs("os.stat(%s)" % f)
            if self.dryrun: continue
            s = timer()
            os.stat(f)
            self.res.append((s, timer()))

        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res 

class stat_non(MetaOp):
    """Access a list of NON-EXIST files by os.stat()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "stat_non", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        nfiles = map(lambda f:f+'n', self.files)
        
        for f in nfiles:
            if self.verbose: self.vs("os.stat(%s)" % f)
            if self.dryrun: continue
            s = timer()
            try: os.stat(f)
            except: pass
            self.res.append((s, timer()))

        if not self.dryrun: assert len(self.res) == len(nfiles)
        return self.res

class utime(MetaOp):
    """Access a list of files by os.utime()"""
    def __init__(self, files, times=None, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "utime", files, verbose, dryrun)
        self.times = times
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose: self.vs("os.utime(%s, %s)" % (f, self.times))
            if self.dryrun: continue
            s = timer()
            os.utime(f, self.times)
            self.res.append((s, timer()))
        
        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res 

class chmod(MetaOp):
    """Access a list of files by os.chmod()"""
    def __init__(self, files, mode=stat.S_IEXEC, verbose=False, dryrun=False,
        **kw):
        MetaOp.__init__(self, "chmod", files, verbose, dryrun)
        self.mode = mode
        self.updatekw(kw)
   
    def execute(self):
        for f in self.files:
            if self.verbose: self.vs("os.chmod(%s, 0x%x)" % (f, self.mode))
            if self.dryrun: continue
            s = timer()
            os.chmod(f, self.mode)
            self.res.append((s, timer()))

        if not self.dryrun: assert len(self.res) == len(self.files)
        return self.res 

class rename(MetaOp):
    """Access a list of files by os.rename()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "rename", files, verbose, dryrun)
        self.updatekw(kw)

    def execute(self):
        for f in self.files:
            if self.verbose: self.vs("os.rename(%s, %s.to)" % (f, f))
            if self.dryrun: continue
            tofile = f + ".to"
            s = timer()
            os.rename(f, tofile)
            self.res.append((s, timer()))
        
        if not self.dryrun:
            assert len(self.res) == len(self.files)
        
            # rename back
            for f in self.files:
                tofile = f + ".to"
                if self.verbose: 
                    self.vs("os.rename(%s, %s) back" % (tofile, f))
                os.rename(tofile, f)

        return self.res

class unlink(MetaOp):
    """Unlink a list of files by os.unlink()"""
    def __init__(self, files, verbose=False, dryrun=False, **kw):
        MetaOp.__init__(self, "unlink", files, verbose, dryrun)
        self.updatekw(kw)
    
    def execute(self):
        for f in self.files:
            if self.verbose:
                self.vs("os.unlink(%s)" % f)
            if self.dryrun:
                continue
            s = timer()
            os.unlink(f)
            self.res.append((s, timer()))
        
        if not self.dryrun: 
            assert len(self.res) == len(self.files)
        return self.res 

class IOOp(Op):
    """I/O operation base class"""
    def __init__(self, name, files, fsize, blksize, flags, verbose=False,
        dryrun=False):
        Op.__init__(self)
        self.type = OPTYPE_IO
        self.name = name
        self.files = files
        self.fsize = fsize
        self.blksize = blksize
        self.flags = flags
        self.verbose = verbose
        self.dryrun = dryrun

FSOP_IO = ["read", "reread", "write", "rewrite", "fread", "freread",
    "fwrite", "frewrite", "offsetread", "offsetwrite"]
__all__.extend(FSOP_IO)

class read(IOOp):
    """Read a files by os.read() with give parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags=os.O_RDONLY, verbose=False,
        dryrun=False, **kw):
        IOOp.__init__(self, "read", files, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("read: os.read(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun:
            return None
        
        # Be careful!
        # The first record is open time and the last record is close time
        ret = 1
        s = timer()
        fd = os.open(self.files, self.flags)
        self.res.append((s, timer()))
        
        while True:
            s = timer()
            ret = os.read(fd, self.blksize)
            e = timer()
            if len(ret) == 0:
                break
            else:
                assert len(ret) == self.blksize
                self.res.append((s, e))
        
        s = timer()
        os.close(fd)
        self.res.append((s, timer()))

        return self.res

class reread(IOOp):
    """Reread a files by os.read() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags=os.O_RDONLY, verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "reread", files, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            self.vs("os.read(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None

        ret = 1
        s = timer()
        fd = os.open(self.files, self.flags)
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

class write(IOOp):
    """write a files by os.write() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags=os.O_CREAT | os.O_RDWR, 
        mode=stat.S_IRUSR | stat.S_IWUSR, byte='0', fsync=False,
        verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "write", files, fsize, blksize, flags, verbose,
        dryrun)
        self.mode = mode
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)
       
    def execute(self):
        if self.verbose:
            self.vs("write: os.write(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun:
            return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        fd = os.open(self.files, self.flags, self.mode)
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

class rewrite(IOOp):
    """Re-write a files by os.write() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags=os.O_CREAT | os.O_RDWR,
        mode=stat.S_IRUSR | stat.S_IWUSR, byte='1', fsync=False, 
        verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "rewrite", files, fsize, blksize, flags, verbose,
        dryrun)
        self.mode = mode
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)

    def execute(self):
        if self.verbose:
            self.vs("os.write(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun:
            return None
        
        block = self.byte * self.blksize
        writebytes = 0
        self.res = []
        s = timer()
        fd = os.open(self.files, self.flags, self.mode)
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

class fread(IOOp):
    """Read a files by f.read() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags='r', verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "fread", files, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.read(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun:
            return None
        
        ret = 1
        s = timer()
        f = _open(self.files, self.flags)
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

class freread(IOOp):
    """Read a files by f.read() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags='r', verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "freread", files, fsize, blksize, flags, verbose,
        dryrun)
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.read(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        ret = 1
        s = timer()
        f = _open(self.files, self.flags)
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

class fwrite(IOOp):
    """write a files by f.write() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags='w', byte='2', fsync=False, verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "fwrite", files, fsize, blksize, flags, verbose,
        dryrun)
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.write(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        f = _open(self.files, self.flags)
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

class frewrite(IOOp):
    """Re-write a files by f.write() with given parameters"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags='w', byte='3', fsync=False, verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "frewrite", files, fsize, blksize, flags, verbose,
        dryrun)
        self.byte = byte
        self.fsync = fsync
        self.updatekw(kw)
    
    def execute(self):
        if self.verbose:
            self.vs("f.write(%s, %d) * %d" 
                % (self.files, self.blksize, self.fsize/self.blksize))
        if self.dryrun: return None
        
        block = self.byte * self.blksize
        writebytes = 0
        s = timer()
        f = _open(self.files, self.flags)
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

class offsetread(IOOp):
    """Read a files by os.read() with offsets in a certain distribution"""
    def __init__(self, files, fsize=1024, blksize=1024, flags=os.O_RDONLY, 
        dist=None, verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "offsetread", files, fsize, blksize, flags, 
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
                    (self.files, self.blksize, self.fsize/self.blksize, 
                     offset))
        if self.dryrun: return None
        
        s = timer()
        fd = os.open(self.files, self.flags)
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

class offsetwrite(IOOp):
    """Write a files by os.write() with offsets in a certain distribution"""
    def __init__(self, files, fsize=1024, blksize=1024, 
        flags=os.O_CREAT | os.O_RDWR, 
        byte='4', dist=None, fsync=False, verbose=False, dryrun=False, **kw):
        IOOp.__init__(self, "offsetwrite", files, fsize, blksize, flags, 
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
                    (self.files, self.blksize, 
                    self.fsize/self.blksize, offset))
        if self.dryrun:
            return None

        block = self.byte * self.blksize
        s = timer()
        fd = os.open(self.files, self.flags)
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
