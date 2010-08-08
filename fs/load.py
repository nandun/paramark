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

# fs/load.py
# Benchmark Load Generation

import os
import random

from modules.verbose import *
import oper

__all__ = ['BenchLoad']

class BenchLoad:
    def __init__(self, cfg=None):
        self.cfg = cfg
        self.dir = '%s/paramark-%03d-%d-%d' % \
            (self.cfg.wdir, random.randint(0,999), self.cfg.hid, self.cfg.pid)
        self.meta = {}

    def generate(self, tid):
        self.threaddir = '%s-%d' % (self.dir, tid)
        load = self.generate_io(tid)
        load.extend(self.generate_meta(tid))
        return self.threaddir, load

    def generate_io(self, tid):
        load = []
        for o in self.cfg.io:
            for fs in self.cfg.fsize:
                for bs in self.cfg.bsize:
                    if o == 'write':
                        op = oper.write(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.write.flags,
                            mode=self.cfg.write.mode,
                            fsync=self.cfg.write.fsync,
                            dryrun=self.cfg.dryrun)
                    elif o == 'rewrite':
                        op = oper.rewrite(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.rewrite.flags,
                            mode=self.cfg.rewrite.mode,
                            fsync=self.cfg.rewrite.fsync,
                            dryrun=self.cfg.dryrun)
                    elif o == 'read':
                        op = oper.read(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.read.flags,
                            mode=self.cfg.read.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'reread':
                        op = oper.reread(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.reread.flags,
                            mode=self.cfg.reread.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'fread':
                        op = oper.fread(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            mode=self.cfg.fread.mode,
                            bufsize=self.cfg.fread.bufsize,
                            dryrun=self.cfg.dryrun)
                    elif o == 'freread':
                        op = oper.freread(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            mode=self.cfg.freread.mode,
                            bufsize=self.cfg.freread.bufsize,
                            dryrun=self.cfg.dryrun)
                    elif o == 'fwrite':
                        op = oper.fwrite(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            mode=self.cfg.fwrite.mode,
                            bufsize=self.cfg.fwrite.bufsize,
                            fsync=self.cfg.fwrite.fsync,
                            dryrun=self.cfg.dryrun)
                    elif o == 'frewrite':
                        op = oper.frewrite(
                            f=self.get_io_load(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            mode=self.cfg.frewrite.mode,
                            bufsize=self.cfg.frewrite.bufsize,
                            fsync=self.cfg.frewrite.fsync,
                            dryrun=self.cfg.dryrun)
                    else:
                        warning("unknow I/O operation \"%s\", ignored" % o)
                        continue
                    load.append(op)
        return load
    
    def generate_meta(self, tid):
        load = []
        for o in self.cfg.meta:
            for ct in self.cfg.opcnt:
                for ft in self.cfg.factor:
                    if o == 'mkdir':
                        op = oper.mkdir(
                            files=self.get_meta_load(tid, ct, ft)[0],
                            opcnt=ct, factor=ft,
                            dryrun=self.cfg.dryrun)
                    elif o == 'rmdir':
                        files = list(self.get_meta_load(tid, ct, ft)[0])
                        files.reverse()
                        op = oper.rmdir(
                            files=files, opcnt=ct, factor=ft,
                            dryrun=self.cfg.dryrun)
                    elif o == 'creat':
                        op = oper.creat(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            flags=self.cfg.creat.flags,
                            mode=self.cfg.creat.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'access':
                        op = oper.access(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            mode=self.cfg.access.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'open':
                        op = oper.open(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            flags=self.cfg.open.flags,
                            mode=self.cfg.open.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'open_close':
                        op = oper.open_close(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            flags=self.cfg.open_close.flags,
                            mode=self.cfg.open_close.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'stat_exist':
                        op = oper.stat_exist(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            dryrun=self.cfg.dryrun)
                    elif o == 'stat_non':
                        op = oper.stat_non(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            dryrun=self.cfg.dryrun)
                    elif o == 'utime':
                        op = oper.utime(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            times=self.cfg.utime.times,
                            dryrun=self.cfg.dryrun)
                    elif o == 'chmod':
                        op = oper.chmod(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            mode=self.cfg.chmod.mode,
                            dryrun=self.cfg.dryrun)
                    elif o == 'rename':
                        op = oper.rename(
                            files=self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            dryrun=self.cfg.dryrun)
                    elif o == 'unlink':
                        op = oper.unlink(
                            files = self.get_meta_load(tid, ct, ft)[1],
                            opcnt=ct, factor=ft,
                            dryrun=self.cfg.dryrun)
                    else:
                        warning("unknow meta operation \"%s\", ignored" % o)
                        continue
                    load.append(op)
        return load

    def get_io_load(self, tid, fsize, bsize):
        return '%s/io-t%d-%d-%d.tmp' % (self.threaddir, tid, fsize, bsize)

    def get_meta_load(self, tid, opcnt, factor):
        key = "%d.%d.%d" % (tid, opcnt, factor)
        if self.meta.has_key(key): return self.meta[key]
        else:
            queue = [ str(self.threaddir) ]
            i = l = 0
            dirs = []
            files = []
            while i < opcnt:
                if i % factor == 0:
                    parent = queue.pop(0)
                    l += 1
                child = os.path.normpath("%s/L%d-%d-%d-%d" 
                    % (parent, l, i, opcnt, factor))
                dirs.append(child)
                files.append("%s/%d-%d.tmp" % (child, l, i))
                queue.append(child)
                i += 1
            self.meta[key] = (dirs, files)
            return self.meta[key]
