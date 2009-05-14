#############################################################################
# ParaMark: High Fidelity Parallel File System Benchmark
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

#############################################
# common constants, routines and utilities
#############################################

import sys
import optparse
import textwrap
import time

PARAMARK_VERSION = 0.1
PARAMARK_DATE = "2009-05-14"

INTEGER_MAX = sys.maxint
INTEGER_MIN = -sys.maxint -1

KB = 1024
MB = 1048576
GB = 1073741824
TB = 1099511627776

OPSET_META = ["mkdir", "rmdir", "creat", "access", "open", "open+close",
              "stat", "stat_NONEXIST", "utime", "chmod", "rename", 
              "unlink"]
OPSET_IO = ["write", "rewrite", "read", "reread", "fwrite", "frewrite", 
            "fread", "freread", "randread", "randwrite"]

OPDATA_META = ["op", "nproc", "factor", "opcnt", "what"]
OPDATA_IO = ["op", "nproc", "fsize", "blksize", "what"]

VERBOSE_CHECKPOINT = 2
VERBOSE_OP = 3
VERBOSE_OP_DETAILS = 4
VERBOSE_FILE_DIR = 3

if sys.platform == "win32":
    timer = time.clock
else:
    timer = time.time

def ws(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def es(s):
    sys.stderr.write(s)
    sys.stderr.flush()

def parse_datasize(size):
    """ return the data size in bytes expressed by size string """
    size = size.upper()
    if size.isdigit():
        return eval(size)
    if size.endswith('B'):
        size = size[0:-1]
    if size.endswith('K'):
        return eval(size[0:-1]) * KB
    if size.endswith('M'):
        return eval(size[0:-1]) * MB
    if size.endswith('G'):
        return eval(size[0:-1]) * GB

def smart_datasize(size):
    """ given a size in bytes, return a tuple (num, unit) """
    size = float(size)
    if size < KB:
        return (size, "B")
    if size < MB:
        return (size/KB, "KB")
    if size < GB:
        return (size/MB, "MB")
    if size < TB:
        return (size/GB, "GB")
    return (size/TB, "TB")

def string_hash(str):
    hash = 0
    for i in range(0, len(str)):
        hash = hash + ord(str[i]) * (i + 1);
    return hash

# list utilities
def list_unique(a):
    """ return the list with duplicate elements removed """
    return list(set(a))

def list_intersect(listoflist):
    """ return the intersection of a series of lists """
    inter = set(listoflist[0])
    for l in listoflist:
        inter = inter & set(l)
    return list(inter)

def list_union(listoflist):
    """ return the union of a series of lists """
    union = set(listoflist[0])
    for l in listoflist:
        union = union | set(l)
    return list(union)

def list_difference(listoflist):
    """ return the difference of a series of lists """
    diff = set(listoflist[0])
    for l in listoflist:
        diff = diff.difference(set(l))
    return list(set(diff))

# statistic functions
def stat_average(listofdata):
    """ return the average of a series of data """
    return sum(listofdata)/float(len(listofdata))

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
