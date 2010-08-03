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

import random

import oper

__all__ = ['BenchLoad']

class BenchLoad:
    def __init__(self, cfg=None):
        self.cfg = cfg
        self.dir = '%s/paramark-%03d-%d-%d' % \
            (self.cfg.wdir, random.randint(0,999), self.cfg.hid, self.cfg.pid)

    def generate(self, tid):
        self.threaddir = '%s-%d' % (self.dir, tid)
        load = []
        for o in self.cfg.io:
            for fs in self.cfg.fsize:
                for bs in self.cfg.bsize:
                    if o == 'write':
                        op = oper.write(
                            f=self.get_io_file(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.write.flags,
                            mode=self.cfg.write.mode,
                            fsync=self.cfg.write.fsync,
                            dryrun=self.cfg.dryrun)
                    elif o == 'rewrite':
                        op = oper.rewrite(
                            f=self.get_io_file(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.rewrite.flags,
                            mode=self.cfg.rewrite.mode,
                            fsync=self.cfg.rewrite.fsync,
                            dryrun=self.cfg.dryrun)
                    elif o == 'read':
                        op = oper.read(
                            f=self.get_io_file(tid, fs, bs),
                            fsize=fs, bsize=bs,
                            flags=self.cfg.read.flags,
                            dryrun=self.cfg.dryrun)
                    load.append(op)
        return self.threaddir, load

    def get_io_file(self, tid, fsize, bsize):
        return '%s/io-t%d-%d-%d.tmp' % (self.threaddir, tid, fsize, bsize)
    
    def get(self):
        return []
