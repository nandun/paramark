#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
#############################################################################

#
# fs/opts.py
# Options and Configurations Parsers
#

import sys
import os
import stat

from modules.opts import Options as CommonOptions
from common.utils import *
from bench import FSOP_META, FSOP_IO

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
        
        self.optParser.add_option("-w", "--wdir", action="store", 
            type="string", dest="wdir", metavar="PATH", default=None,
            help="working directory (default: cwd)")
        
        self.optParser.add_option("-l", "--logdir", action="store", 
            type="string", dest="logdir", metavar="PATH", default=None,
            help="log directory (default: auto)")
        
        self.optParser.add_option("-t", "--threads", action="store", 
            type="int", dest="nthreads", metavar="NUM", default=None,
            help="number of current threads (default: 1)")
        
        self.optParser.add_option("-f", "--force", action="store_false",
            dest="confirm", default=True,
            help="Force to go, do not confirm (default: disabled)")

    def _parse_conf(self, fp, filename=[]):
        if fp:
            self.cfgParser.readfp(fp)
        if filename:
            loaded_files = self.cfgParser.read(filename)

        # MUST keep consistent with configure file format
        for section in ["runtime"]:
            if self.cfgParser.has_section(section):
                for k, v in self.cfgParser.items(section):
                    self.set_val(k, eval(v))
        
        # Configuration for each operation
        for op in FSOP_META + FSOP_IO:
            if self.cfgParser.has_section(op):
                self.set_subval(op, map(lambda (k,v):(k,eval(v)),
                    self.cfgParser.items(op)))

        # Override local options
        for sec in ["meta", "io"]:
            section = sec + "opts"
            self.set_subval(section,
                map(lambda (k,v):(k,eval(v)), self.cfgParser.items(section)))
            if self.cfgParser.has_section(section) and \
               self.cfgParser.has_option(section, "overwrite") and \
               self.cfgParser.getboolean(section, "overwrite"):
                for k, v in self.cfgParser.items(sec + "opts"):
                    for op in self.opts[sec + "ops"]:
                        if self.opts[op].has_key(k):
                            self.opts[op][k] = eval(v)
        
        if filename:
          return loaded_files
        return None

    def _load(self):
        errstr = None

        # Load from command options
        # section runtime
        self.opts["report"] = opts.report
        
        # Check options here
        for o in self.opts["metaops"] + self.opts["ioops"]:
            if o not in FSOP_META + FSOP_IO:
                errstr = "invalid filesystem operation %s" % o
        
        # Rearrange operation sequence based on dependencies
        if len(self.opts["metaops"]) > 0:
            _metaops = ["mkdir", "rmdir"]
            if len(list_intersect([["creat", "access", "open", "open_close",
                "stat_exist", "stat_non", "utime", "chmod", "rename", 
                "unlink"],
                self.opts["metaopts"]])) > 0:
                _metaops.insert(-1, "creat")
            
            for o in self.opts["metaops"]:
                if o not in _metaops:
                    _metaops.insert(-1, o)
            self.opts["metaops"] = _metaops

        _ioops = ["write"]
        for o in self.opts["ioops"]:
            if o not in _ioops:
                _ioops.append(o)
        self.opts["ioops"] = _ioops
        
        section = "runtime"
        for o in ["wdir", "logdir", "nthreads", 
            "confirm", "verbosity", "dryrun"]: 
            # refer above for load options
            if opts.__dict__[o] is not None:
                self.cfg.set(section, o, "%s" % opts.__dict__[o])
                self.opts[o] = opts.__dict__[o]
        self.opts["wdir"] = os.path.abspath(self.opts["wdir"])

        if self.opts["verbosity"] >= 5 and loaded_files is not None:
            sys.stderr.write("Successfull load configuration from %s.\n" %
                ", ".join(loaded_files))

        return self.opts, errstr

##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

FS_BENCHMARK_DEFAULT_CONFIG_STRING = """\
# ParaMark Default Benchmarking Configuration
# last updated: 2010/03/26

##########################################################################
# Howto:
#   * Only modify the values you would like to change.
#   * Lines beginning with '#' or ';' are ignored.
#   * Every value should be an evaluable string by Python.
#     For example, a path value should be embraced by quotation marks, 
#     i.e., '/full/path' or "/full/path"
#   * Following the conventions of this file will be safe.
##########################################################################

##########################################################################
# Global Options
##########################################################################
[global]
# Benchmark working directory
# Don't forget quotation marks: " "
wdir = "./"

# Number of concurrent benchmarking thread
nthreads = 1

# Ask user whether to proceed on critical situations
confirm = True

# Verbosity level (0-5)
verbosity = 0

# Dryrun, do nothing
dryrun = False

# Log directory of benchmarking results
# Generate a random log directory when logdir is None
logdir = None

# Metadata operations to be performed
# Does not support line continuation now, keep option in one line
meta = ["mkdir", "rmdir", "creat", "access", "open", "open_close", \
"stat_exist", "stat_non", "utime", "chmod", "rename", "unlink"]

# I/O operations to be performed
io = ["read", "reread", "write", "rewrite", "fread", "freread", \
"fwrite", "frewrite", "offsetread", "offsetwrite"]

# Overwrite following local settings
override=True

# Variables to override
opcnt = 10
factor = 16

# K=1024, M=1048576, G=1073741824, T=1099511627776
fsize = 10 * 1048576
bsize = 4 * 1024

##########################################################################
# Local Operation Options
#   * Safe to leave alone
#   * Each operation in a seperate section
##########################################################################

#
# Options for flags
# os.O_RDONLY, os.O_WRONLY, os.RDWR, os.O_APPEND, os.O_CREAT, os.O_EXCL
# os.O_TRUNC or their inclusive OR
#
# Options for mode
# stat.S_ISUID, stat.S_ISGID, stat.S_ENFMT, stat.S_ISVTX, stat.S_IREAD,
# stat.S_IWRITE, stat.S_IEXEC, stat.S_IRWXU, stat.S_IRUSR, stat.S_IWUSR,
# stat.S_IXUSR, stat.S_IRWXG, stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP,
# stat.S_IRWXO, stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH or their bitwise OR
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
flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC 
mode = stat.S_IRUSR | stat.S_IWUSR
factor = 16

[access]
opcnt = 0
# os.F_OK, os.R_OK, os.W_OK, os.X_OK or their inclusive OR
mode = os.F_OK
factor = 16

[open]
opcnt = 0
flags = os.O_RDONLY
factor = 16

[open_close]
opcnt = 0
flags = os.O_RDONLY
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
chmod = stat.S_IEXEC
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
flags = os.O_RDONLY

[reread]
fsize = 0
bsize = 0
flags = os.O_RDONLY

[write]
fsize = 0
bsize = 0
flags = os.O_CREAT | os.O_RDWR
mode = stat.S_IRUSR | stat.S_IWUSR
byte = '0'
fsync = False

[rewrite] 
fsize = 0
bsize = 0
flags = os.O_CREAT | os.O_RDWR
mode = stat.S_IRUSR | stat.S_IWUSR
byte = '1'
fsync = False

[fread]
fsize = 0
bsize = 0
# 'r', 'w', or 'a'
flags = 'r'

[freread]
fsize = 0
bsize = 0
flags = 'r'

[fwrite]
fsize = 0
bsize = 0
flags = 'w'
byte = '2'
fsync = False

[frewrite]
fsize = 0
bsize = 0
flags = 'w'
byte = '3'
fsync = False

[offsetread]
fsize = 0
bsize = 0
flags = os.O_RDONLY

[offsetwrite]
fsize = 0
bsize = 0
flags = os.O_CREAT | os.O_RDWR
byte = '4'
fsync = False
"""

# EOF
