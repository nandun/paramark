#############################################################################
# ParaMark:  A Parallel/Distributed File Systems Benchmark
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
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
# Options and Configurations Parsers
#

import sys
import os
import stat
import optparse
import ConfigParser

from fsop import FSOP_META, FSOP_IO

class Options:
    """Store/Retrieve options from/to configure files or command arguments
    """
    def __init__(self):
        self.cfg = ConfigParser.ConfigParser()
        
        # options container to pass to benchmark
        self.opts = {}

    #
    # Parse/Store options from/to configure file
    #
    def default_conf(self, filename=None):
        if filename:
            try:
                f = open(filename, "wb")
            except IOError:
                sys.stderr.write("failed to open file %s" % filename)
                sys.exit(1)
            f.write(PARAMARK_DEFAULT_CONFIG_FILE)
            f.close()
        else:
            sys.stdout.write(PARAMARK_DEFAULT_CONFIG_FILE)
        
    def parse_conf(self, filename):
        # MUST keep consistent with configure file format
        self.cfg.read(filename)

        section = "runtime"
        if self.cfg.has_section(section):
            for k, v in self.cfg.items(section):
                self.opts[k] = eval(v)
        
        # Configuration for each operation
        oplist = self.opts["metaops"] + self.opts["ioops"]
        for op in oplist:
            if self.cfg.has_section(op):
                self.opts[op] = {}
                for k, v in self.cfg.items(op):
                    self.opts[op][k] = eval(v)

        # Override local options
        for sec in ["meta", "io"]:
            if self.cfg.has_section(sec + "opts"):
                for k, v in self.cfg.items(sec + "opts"):
                    for op in self.opts[sec + "ops"]:
                        if self.opts[op].has_key(k):
                            self.opts[op][k] = eval(v)

    def store_conf(self, filename="paramark.conf"):
        return
        
    #
    # Parse/Store options from/to command line
    #
    def parse_argv(self, argv):
        return

#
# Default configure file
# Hard-coded for installation convenience
#

PARAMARK_DEFAULT_CONFIG_FILE = """\
# ParaMark default benchmarking configuration
# 2009/12/17

##########################################################################
# Howto:
#   * Only modify the values you would like to change
#   * Lines beginning with '#' or ';' are ignored
#   * Every value should be an evaluable string by Python.
#     For example, a path value should be embraced by quotation marks, 
#     e.g. '/full/path' or "/full/path"
#     Following the convention in this file will be safe.
##########################################################################

##########################################################################
# Global Runtime Options
##########################################################################

[runtime]
# Benchmark working directory
# Don't forget the quotation marks: " "
wdir = "./"

# Number of concurrent benchmarking thread
nthread = 0

# Verbosity level (0-5)
verbosity = 0

# Dryrun, do nothing
dryrun = False

# Log directory of benchmarking results
logdir = None

# Metadata operations to be performed
metaops = ["mkdir", "rmdir", "creat", "access", "open", "open_close", \
"stat_exist", "stat_non", "utime", "chmod", "rename", "unlink"]

# I/O operations to be performed
ioops = ["read", "reread", "write", "rewrite", "fread", "freread", \
"fwrite", "frewrite", "offsetread", "offsetwrite"]

[report]
timing-include-open = True
timing-include-close = True

##########################################################################
# Globale options to override local ones
##########################################################################
[metaopts]
# overwrite following local settings
overwrite=True

# list variables to override
opcnt = 1000

[ioopts]
# overwrite following local settings
overwrite=True

# list variables to override
# K=1024, M=1048576, G=1073741824, T=1099511627776
fsize = 10 * 1048576
bsize = 4 * 1024

##########################################################################
# Local Operation Options
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

[rmdir]
opcnt = 0

[creat]
opcnt = 0
flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC 
mode = stat.S_IRUSR | stat.S_IWUSR

[access]
opcnt = 0
# os.F_OK, os.R_OK, os.W_OK, os.X_OK or their inclusive OR
mode = os.F_OK

[open]
opcnt = 0
flags = os.O_RDONLY

[open_close]
opcnt = 0
flags = os.O_RDONLY

[stat_exist]
opcnt = 0

[stat_non]
opcnt = 0

[utime]
opcnt = 0
times = None

[chmod]
opcnt = 0
chmod = stat.S_IEXEC

[rename]
opcnt = 0

[unlink]
opcnt = 0

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

[rewrite] 
fsize = 0
bsize = 0
flags = os.O_CREAT | os.O_RDWR
mode = stat.S_IRUSR | stat.S_IWUSR

[offsetread]
fsize = 0
bsize = 0
flags = os.O_RDONLY

[offsetwrite]
fsize = 0
bsize = 0
flags = os.O_CREAT | os.O_RDWR

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

[frewrite]
fsize = 0
bsize = 0
flags = 'w'
"""

if __name__ == "__main__":
    c = Options()
    #c.store_conf()
    c.default_conf("paramark.conf")
    c.parse_conf("paramark.conf")
