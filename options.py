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
import textwrap
import ConfigParser
import StringIO

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
    def print_default_conf(self, filename=None):
        if filename:
            try:
                f = open(filename, "wb")
            except IOError:
                sys.stderr.write("failed to open file %s" % filename)
                sys.exit(1)
            f.write(PARAMARK_DEFAULT_CONFIG_STRING)
            f.close()
        else:
            sys.stdout.write(PARAMARK_DEFAULT_CONFIG_STRING)
        
    def parse_conf(self, fp, filename=[]):
        if fp:
            self.cfg.readfp(fp)
        if filename:
            loaded_files = self.cfg.read(filename)

        # MUST keep consistent with configure file format
        for section in ["runtime", "report"]:
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
            section = sec + "opts"
            if self.cfg.has_section(section) and \
               self.cfg.has_option(section, "overwrite") and \
               self.cfg.getboolean(section, "overwrite"):
                for k, v in self.cfg.items(sec + "opts"):
                    for op in self.opts[sec + "ops"]:
                        if self.opts[op].has_key(k):
                            self.opts[op][k] = eval(v)
        
        if filename: return loaded_files
        return None

    def save_conf(self, filename):
        """Save current configuration to file"""
        fp = open(filename, "wb")
        self.cfg.write(fp)
        fp.close()
        
    #
    # Parse/Store options from/to command line
    #
    def parse_argv(self, argv):
        usage = "paramark command [options]"

        parser = optparse.OptionParser(usage=usage,
                    formatter=OptionParserHelpFormatter())
        
        #parser.remove_option("-h")
        #parser.add_option("-h", "--help", action="store_true",
        #    dest="help", default=False, help="show the help message and exit")
        
        parser.add_option("-c", "--conf", action="store", type="string",
            dest="conf", metavar="PATH", default="",
            help="configuration file")
        
        parser.add_option("-p", "--print-default-conf", action="store_true",
            dest="printdefaultconf", default=False, 
            help="print default configuration file and exit")
        
        # override configuration part
        parser.add_option("-w", "--wdir", action="store", type="string",
            dest="wdir", metavar="PATH", default="./",
            help="working directory (default: cwd)")
        
        parser.add_option("-l", "--logdir", action="store", type="string",
            dest="logdir", metavar="PATH", default=None,
            help="log directory (default: auto)")
        
        parser.add_option("-f", "--force", action="store_false",
            dest="confirm", default=True,
            help="Force to go, do not confirm (default: disabled)")
        
        parser.add_option("-v", "--verbosity", action="store", type="int",
            dest="verbosity", metavar="NUM", default=None,
            help="verbosity level: 0/1/2/3/4/5 (default: 0)")
        
        parser.add_option("-d", "--dryrun", action="store_true",
            dest="dryrun", default=None, 
            help="dry run, do not execute (default: disabled)")

        opts, args = parser.parse_args(argv)

        return opts, None

    def load(self, argv):
        errstr = None

        # Parse command options first, and handling some common options
        opts, _ = self.parse_argv(argv)
        if opts.printdefaultconf:
            self.print_default_conf()
            sys.exit(0)

        output = StringIO.StringIO(PARAMARK_DEFAULT_CONFIG_STRING)
        loaded_files = self.parse_conf(output,          # load hard string
            [os.path.expanduser("~/.paramark_conf"),    # load home default
             os.path.abspath(".paramark_conf"),         # load cwd default
             os.path.abspath(opts.conf)])               # load user-specified
        output.close()

        # Load from command options
        # section runtime
        section = "runtime"
        for o in ["wdir", "logdir", "confirm", "verbosity", "dryrun"]: 
            # refer above for load options
            if opts.__dict__[o] is not None:
                self.cfg.set(section, o, "%s" % opts.__dict__[o])
                self.opts[o] = opts.__dict__[o]

        if self.opts["verbosity"] >= 5 and loaded_files is not None:
            sys.stderr.write("Successfull load configuration from %s.\n" %
                ", ".join(loaded_files))

        return self.opts, errstr

# OptionParser help string workaround
# adapted from Tim Chase's code from following thread
# http://groups.google.com/group/comp.lang.python/msg/09f28e26af0699b1
class OptionParserHelpFormatter(optparse.IndentedHelpFormatter):
    def format_description(self, desc):
        if not desc: return ""
        desc_width = self.width - self.current_indent
        indent = " " * self.current_indent
        bits = desc.split('\n')
        formatted_bits = [
            textwrap.fill(bit, desc_width, initial_indent=indent,
                susequent_indent=indent)
            for bit in bits]
        result = "\n".join(formatted_bits) + "\n"
        return result

    def format_option(self, opt):
        result = []
        opts = self.option_strings[opt]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if opt.help:
            help_text = self.expand_default(opt)
            help_lines = []
            for para in help_text.split("\n"):
                help_lines.extend(textwrap.wrap(para, self.help_width))
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line)
                for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)

##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

PARAMARK_DEFAULT_CONFIG_STRING = """\
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
