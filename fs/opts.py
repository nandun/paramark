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

#
# fs/opts.py
# Options and Configurations Parsers
#

import sys
import os
import stat

from modules.utils import *
from modules.opts import Options as CommonOptions

class Options(CommonOptions):
    """
    Store/Retrieve options from/to configure files or command arguments
    """
    def __init__(self, argv=None):
        CommonOptions.__init__(self, argv)
        self.DEFAULT_CONFIG_STRING = FS_BENCHMARK_DEFAULT_CONFIG_STRING
    
    def _add_default_options(self):
        CommonOptions._add_default_options(self)
        
        # Should keep default value None here, since we need to test
        # whether an option has been set by command
        # instead, default values are set from DEFAULT_CONFIG_STRING
        self.optParser.add_option("-r", "--report", action="store", 
            type="string", dest="report", metavar="PATH", default=None, 
            help="generate report from log directory")
        
        self.optParser.add_option("-n", "--no-report", action="store_true",
            dest="noreport", default=False,
            help="do NOT report after benchmarking (default: disabled)")
        
        self.optParser.add_option("-w", "--wdir", action="store", 
            type="string", dest="wdir", metavar="PATH", default=None,
            help="working directory (default: cwd)")
        
        self.optParser.add_option("-l", "--logdir", action="store", 
            type="string", dest="logdir", metavar="PATH", default=None,
            help="log directory (default: auto)")
        
        self.optParser.add_option("-t", "--threads", action="store", 
            type="int", dest="nthreads", metavar="NUM", default=None,
            help="number of concurrent threads (default: 1)")
        
        self.optParser.add_option("-f", "--force", action="store_false",
            dest="confirm", default=True,
            help="force to go, do not confirm (default: disabled)")
        
        self.optParser.add_option("--quick-report", action="store_true",
            dest="quickreport", default=False,
            help="quick report, does not save any data (default: disabled)")
        
        self.optParser.add_option("--text-report", action="store_true",
            dest="textreport", default=False,
            help="generate text report (default: disabled)")
        
        self.optParser.add_option("--no-log", action="store_true",
            dest="nolog", default=False,
            help="do NOT save log, create report only (default: disabled)")
    
    def _invalid_val(self, opt, val):
        from os import O_RDONLY, O_WRONLY, O_RDWR, O_APPEND,O_CREAT, \
            O_EXCL, O_TRUNC, F_OK, R_OK, W_OK, X_OK
        from stat import S_ISUID, S_ISGID, S_ENFMT, S_ISVTX, \
            S_IREAD, S_IWRITE, S_IEXEC, S_IRWXU, S_IRUSR, S_IWUSR, \
            S_IXUSR, S_IRWXG, S_IRGRP, S_IWGRP, S_IXGRP, S_IRWXO, \
            S_IROTH, S_IWOTH, S_IXOTH
        from oper import OPS_META, OPS_IO
        
        if opt == "verbosity": return int(val)
        elif opt == "dryrun": return bool(eval(val))
        elif opt == "nthreads": return int(val)
        elif opt == "confirm": return bool(val)
        elif opt == "opcnt": return int(val)
        elif opt == "factor": return int(val)
        elif opt == "fsize":
            return map(lambda v:parse_datasize(v),
                val.split(','))
        elif opt == "bsize":
            return map(lambda v:parse_datasize(v),
                val.split(','))
        elif opt == "flags":
            if val.startswith("O_"): return eval(val)
            else: return str(val)
        elif opt == "mode": return eval(val)
        elif opt == "meta":
            meta = []
            for m in val.split(','):
                if m in OPS_META: meta.append(m)
            return meta
        elif opt == "io":
            io = []
            for o in val.split(','):
                if o in OPS_IO: io.append(o)
            return io
        return val
    
    def check_values(self):
        # Rearrange operation sequence according to dependencies
        if len(self.opts.meta) > 0:
            _meta = ["mkdir", "rmdir"]
            if len(list_intersect([["creat", "access", "open", "open_close",
                "stat_exist", "stat_non", "utime", "chmod", "rename", 
                "unlink"], self.opts.meta])) > 0:
                _meta.insert(-1, "creat")
                _meta.insert(-1, "unlink")
            
            for o in self.opts.meta:
                if o not in _meta:
                    _meta.insert(-2, o)
            self.opts.meta = _meta
        
        if len(self.opts.io) > 0:
            _io = ["write"]
            for o in self.opts.io:
                if o not in _io:
                    _io.append(o)
            self.opts.io = _io
        
##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

FS_BENCHMARK_DEFAULT_CONFIG_STRING = """\
# ParaMark Default Benchmarking Configuration
# last updated: 2010/08/03

##########################################################################
# Howto:
#   * Only modify the values you would like to change.
#   * Lines beginning with '#' or ';' are ignored.
#   * Following the conventions of this file will be safe.
##########################################################################

##########################################################################
# Global Options
##########################################################################
[global]
# Benchmark working directory
# Don't forget quotation marks: " "
wdir = ./

# Number of concurrent benchmarking thread
nthreads = 1

# Ask user whether to proceed on critical situations
confirm = True

# Verbosity level (0-5)
verbosity = 0

# Dryrun, do nothing
dryrun = False

# Log directory of benchmarking results
# Generate a random log directory when logdir is not set
logdir =

# Metadata operations to be performed
# Does not support line continuation now, keep option in one line
# e.g., meta = , meta = mkdir,rmdir,creat,access,open,open_close,stat_exist,stat_non,utime,chmod,rename,unlink
meta =

# I/O operations to be performed
# e.g., io = , io = read,reread,write,rewrite,fread,freread,fwrite,frewrite,offsetread,offsetwrite
io = write

# Overwrite following local settings
override = True

# Variables to override
opcnt = 10
factor = 16

# File size and block size
# e.g., fsize=1K,2M,3G, bsize=1KB,2mb,3gb
fsize = 1M
bsize = 1K

##########################################################################
# Local Operation Options
#   * Safe to leave alone
#   * Each operation in a seperate section
##########################################################################

#
# Options for flags
# O_RDONLY, O_WRONLY, RDWR, O_APPEND, O_CREAT, O_EXCL
# O_TRUNC or their inclusive OR
#
# Options for mode
# S_ISUID, S_ISGID, S_ENFMT, S_ISVTX, S_IREAD,
# S_IWRITE, S_IEXEC, S_IRWXU, S_IRUSR, S_IWUSR,
# S_IXUSR, S_IRWXG, S_IRGRP, S_IWGRP, S_IXGRP,
# S_IRWXO, S_IROTH, S_IWOTH, S_IXOTH or their bitwise OR
#

# Metadata operation
[mkdir]
opcnt = 0
factor = 16

[rmdir]
opcnt = 0
factor = 16

[creat]
opcnt = 0
flags = O_CREAT | O_WRONLY | O_TRUNC 
mode = S_IRUSR | S_IWUSR
factor = 16

[access]
opcnt = 0
# F_OK, R_OK, W_OK, X_OK or their inclusive OR
mode = F_OK
factor = 16

[open]
opcnt = 0
flags = O_RDONLY
factor = 16

[open_close]
opcnt = 0
flags = O_RDONLY
factor = 16

[stat_exist]
opcnt = 0
factor = 16

[stat_non]
opcnt = 0
factor = 16

[utime]
opcnt = 0
times = None
factor = 16

[chmod]
opcnt = 0
chmod = S_IEXEC
factor = 16

[rename]
opcnt = 0
factor = 16

[unlink]
opcnt = 0
factor = 16

# I/O operation
[read]
fsize = 0
bsize = 0
flags = O_RDONLY

[reread]
fsize = 0
bsize = 0
flags = O_RDONLY

[write]
fsize = 0
bsize = 0
flags = O_CREAT | O_RDWR
mode = S_IRUSR | S_IWUSR
fsync = False

[rewrite] 
fsize = 0
bsize = 0
flags = O_CREAT | O_RDWR
mode = S_IRUSR | S_IWUSR
fsync = False

[fread]
fsize = 0
bsize = 0
# 'r', 'w', or 'a'
flags = r

[freread]
fsize = 0
bsize = 0
flags = r

[fwrite]
fsize = 0
bsize = 0
flags = w
fsync = False

[frewrite]
fsize = 0
bsize = 0
flags = w
fsync = False

[offsetread]
fsize = 0
bsize = 0
flags = O_RDONLY

[offsetwrite]
fsize = 0
bsize = 0
flags = O_CREAT | O_RDWR
fsync = False
"""
