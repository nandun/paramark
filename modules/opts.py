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

# modules/opts.py
# Base Option and Configure Parser

import sys
import os
import optparse
import textwrap
import ConfigParser
import StringIO

from verbose import *
from common import *

VERBOSE = 3
VERBOSE_MORE = VERBOSE + 1
VEROBSE_ALL = VERBOSE_MORE + 1

class Options:
    def __init__(self, argv=None):
        self.optParser = optparse.OptionParser(formatter=HelpFormatter())
        self.cfgParser = ConfigParser.ConfigParser()
        self._add_default_options()
        
        # options including command arguments and configuration files
        self.vals = Values()
        self.args = None    # remaining command arguments
        self.DEFAULT_CONFIG_STRING = ""
        self.DEFAULT_CONF_FILENAME = "paramark.conf"
        self.CONF_GLOBAL_SECTION = "global"

        if argv is not None:
            self.parse_argv(argv)
    
    def _add_default_options(self):
        # Override default help behavior
        self.optParser.remove_option("-h")
        self.optParser.add_option("-h", "--help", action="store_true",
            dest="help", default=False,
            help="show this help message and exit")
        
        self.optParser.add_option("-p", "--print-default-conf", 
            action="store_true", dest="printconf", default=False, 
            help="print default configuration file and exit")
        
        self.optParser.add_option("-g", "--gxp", action="store_true",
            dest="gxpmode", default=False,
            help="execute in GXP mode (default: disabled)")
        
        self.optParser.add_option("-c", "--conf", action="store", 
            type="string", dest="conf", metavar="PATH", default="",
            help="configuration file")
        
        self.optParser.add_option("--debug", action="store_true",
            dest="debug", default=False, 
            help="debug mode, print debug information (default: disabled)")
        
        # Options may be set in configuration file
        # default values should be set to None
        self.optParser.add_option("-v", "--verbosity", action="store",
            type="int", dest="verbosity", metavar="NUM", default=None,
            help="verbosity level: 0/1/2/3/4/5 (default: 0)")
        
        self.optParser.add_option("-d", "--dryrun", action="store_true",
            dest="dryrun", default=None, 
            help="dry run, do not execute (default: disabled)")
        
    def _valid_val(self, opt, val):
        if opt == "verbosity": return int(val)
        elif opt == "dryrun": return bool(eval(str(val)))
        return val
                
    def has(self, opt):
        return hasattr(self.vals, opt)

    def set(self, opt, val):
        setattr(self.vals, opt, val)

    def get(self, opt):
        return getattr(self.vals, opt)

    def set_subval(self, opt, subval):
        self.vals.set(opt, Values(subval))

    def set_usage(self, usage):
        self.optParser.set_usage(usage)

    def parse_argv(self, argv):
        opts, self.args = self.optParser.parse_args(argv)
        self.vals.update(opts.__dict__)

    def print_help(self):
        self.optParser.print_help()

    def print_default_conf(self, filename=None):
        sys.stdout.write(self.DEFAULT_CONFIG_STRING)
        sys.stdout.flush()

    def parse_conf(self):
        fp = StringIO.StringIO(self.DEFAULT_CONFIG_STRING)
        self.cfgParser.readfp(fp)
        fp.close()

        loaded_files = self.cfgParser.read(
            [os.path.expanduser("~/%s" % self.DEFAULT_CONF_FILENAME),
             os.path.abspath(".%s" % self.DEFAULT_CONF_FILENAME),
             os.path.abspath(self.vals.conf)])
        
        if loaded_files is not None:
            verbose("Successfull load configurations from %s." %
                    ", ".join(loaded_files), VERBOSE)

    def override_conf(self):
        """
        Override read configurations from command line options
        """
        conf_sections = self.cfgParser.sections()
       
        # Global section
        sec = self.CONF_GLOBAL_SECTION
        for o, v in self.cfgParser.items(sec):
            if self.has(o):
                val = self.get(o)
                if val is None:
                    # Not set by commad line yet, set by default value
                    self.set(o, v)
                else:
                    # Set by command line, override default value
                    self.cfgParser.set(sec, o, str(val))
            else:
                # Set default value
                self.set(o, v)

        # Local sections
        local_secs = list(conf_sections)
        local_secs.remove(self.CONF_GLOBAL_SECTION)
        for sec in local_secs:
            if self.vals.override:
                for o, v in self.cfgParser.items(sec):
                    if self.has(o):
                        verbose(" %s.%s=%s overrided to %s" %
                            (sec, o, v, self.get(o)), VERBOSE_MORE)
                        self.cfgParser.set(sec, o, str(self.get(o)))
            self.set_subval(sec, self.cfgParser.items(sec))
    
    def save_conf(self, filename):
        fp = open(filename, "wb")
        self.cfgParser.write(fp)
        fp.close()

    def validate_values(self, values=None):
        if values is None: values = self.vals
        for o, v in values.items():
            if isinstance(v, Values):
                self.validate_values(v)
            else:
                values.set(o, self._valid_val(o, v))
            
    def load(self):
        if self.vals.help:
            self.print_help()
            sys.exit(0)
        
        if self.vals.printconf:
            self.print_default_conf()
            sys.exit(0)
        
        import verbose
        verbose.verbosity = self.vals.verbosity
        
        self.parse_conf()
 
        self.override_conf()
        
        self.validate_values()
        
        if self.vals.debug:
            for k, v in self.vals.items():
                if isinstance(v, Values):
                    for sk, sv in v.items():
                        sys.stdout.write("debug: opt=%s.%s, val=%s, type=%s\n"
                            % (k, sk, sv, type(sv)))
                else:
                    sys.stdout.write("debug: opt=%s, val=%s, type=%s\n" 
                        % (k, v, type(v)))
                sys.stdout.flush()


FS_BENCHMARK_DEFAULT_CONFIG_STRING = """\
[global]
# Verbosity level (0-5)
verbosity = 0

# Dryrun, do nothing
dryrun = False
"""
        
# OptionParser help string workaround
# adapted from Tim Chase's code from following thread
# http://groups.google.com/group/comp.lang.python/msg/09f28e26af0699b1
class HelpFormatter(optparse.IndentedHelpFormatter):
    def format_description(self, desc):
        if not desc: return ""
        desc_width = self.width - self.current_indent
        indent = " " * self.current_indent
        bits = desc.split('\n')
        formatted_bits = [
            textwrap.fill(bit, desc_width, initial_indent=indent,
                susequent_indent=indent) for bit in bits]
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
