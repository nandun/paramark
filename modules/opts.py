#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
#############################################################################

#
# modules/opts.py
#

import sys
import os
import optparse
import textwrap
import ConfigParser
import StringIO

class Options:
    """
    Common options
    """
    def __init__(self, argv=None):
        self.optParser = optparse.OptionParser(formatter=HelpFormatter())
        self.cfgParser = ConfigParser.ConfigParser()
        self._add_default_options()
        
        # options including command arguments and configuration files
        self.opts = Values()
        self.args = None    # remaining command arguments
        self.DEFAULT_CONFIG_STRING = ""
        self.DEFAULT_CONF_FILENAME = "paramark_conf"
        self.CONF_GLOBAL_SECTION = "global"

        if argv is not None:
            self.parse_argv(argv)
    
    def _add_default_options(self):
        # Reset help option such that it does not call print_help() callback
        self.optParser.remove_option("-h")
        self.optParser.add_option("-h", "--help", action="store_true",
            dest="help", default=False,
            help="show this help message and exit")
        
        self.optParser.add_option("-v", "--verbosity", action="store",
            type="int", dest="verbosity", metavar="NUM", default=0,
            help="verbosity level: 0/1/2/3/4/5 (default: 0)")
        
        self.optParser.add_option("-d", "--dryrun", action="store_true",
            dest="dryrun", default=False, 
            help="dry run, do not execute (default: disabled)")
        
        self.optParser.add_option("-g", "--gxp", action="store_true",
            dest="gxpmode", default=False,
            help="execute in GXP mode (default: disabled)")
        
        self.optParser.add_option("-p", "--print-default-conf", 
            action="store_true", dest="printconf", default=False, 
            help="print default configuration file and exit")

    def has_opt(self, opt):
        return self.opts.has_attr(opt)

    def set_val(self, opt, val):
        """
        Set value of given option
        """
        self.opts.set_value(opt, val)

    def set_subval(self, opt, subval):
        """
        Set value of given options from dict/list
        """
        self.opts.set_value(opt, Values(subval))

    def set_usage(self, usage):
        self.optParser.set_usage(usage)

    def parse_argv(self, argv):
        print "parse argv"
        opts, self.args = self.optParser.parse_args(argv)
        self.opts.update(opts.__dict__)
        return self.opts, self.args

    def print_help(self):
        self.optParser.print_help()

    def print_default_conf(self, filename=None):
        if filename:
            try:
                f = open(filename, "wb")
            except IOError:
                sys.stderr.write("failed to open file %s" % filename)
                sys.exit(1)
            f.write(self.DEFAULT_CONFIG_STRING)
            f.close()
        else:
            sys.stdout.write(self.DEFAULT_CONFIG_STRING)

    def prompt_and_exit(self):
        """
        Prompt user and exit if required
        """
        if self.opts.help:
            self.print_help()
            sys.exit(0)
        if self.opts.printconf:
            self.print_default_conf()
            sys.exit(0)
    
    def parse_conf(self):
        fp = StringIO.StringIO(self.DEFAULT_CONFIG_STRING)
        self.cfgParser.readfp(fp)
        fp.close()
        loaded_files = self.cfgParser.read(
            [os.path.expanduser("~/%s" % self.DEFAULT_CONF_FILENAME),
             os.path.abspath(".%s" % self.DEFAULT_CONF_FILENAME),
             os.path.abspath(self.opts.conf)])

        if self.opts.verbosity >= 5 and loaded_files is not None:
            sys.stderr.write("Successfull load configuration from %s.\n" %
                ", ".join(loaded_files))
        
    def load(self):
        self.prompt_and_exit()
        self.parse_conf()

class Values:
    """
    Option values container
    """
    def __init__(self, values=None):
        if isinstance(values, list):
            for k, v in values:
                setattr(self, k, v)
        elif isinstance(values, dict):
            for k, v in values.items():
                setattr(self, k, v)

    def __str__(self):
        return str(self.__dict__)

    def update(self, dict):
        self.__dict__.update(dict)

    def has_attr(self, attr):
        return self.__dict__.has_key(attr)
        
    def set_value(self, opt, val):
        self.__dict__[opt] = val
        
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
