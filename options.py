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

import os
import stat
import optparse
import ConfigParser

class ParamarkOptions:
    def __init__(self):
        self.cfg = ConfigParser.ConfigParser()
    
    #
    # Parse/Store options from/to configure file
    #
    def parse_conf(self, filename):
        return

    def store_conf(self, filename="paramark.conf"):
        # runtime
        section = "runtime"
        self.cfg.add_section(section)
        self.cfg.set(section, "loadconf", False)
        self.cfg.set(section, "runmode", "auto")
        self.cfg.set(section, "wdir", "./")
        self.cfg.set(section, "verbosity", 0)
        self.cfg.set(section, "dryrun", False)
        self.cfg.set(section, "nthread", 0)

        # metaop
        section = "metaop"
        self.cfg.add_section(section)
        self.cfg.set(section, "metaops", 0)
        self.cfg.set(section, "opcnt", 0)
        
        # ioop
        section = "ioop"
        self.cfg.add_section(section)
        self.cfg.set(section, "ioops", 0)
        self.cfg.set(section, "fsize", 0)
        self.cfg.set(section, "bsize", 0)
        self.cfg.set(section, "timing-without-open", False)
        self.cfg.set(section, "timing-without-close", False)
        
        # flags
        section = "flags"
        self.cfg.add_section(section)
        self.cfg.set(section, "creat", os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        self.cfg.set(section, "open", os.O_RDONLY)
        self.cfg.set(section, "open_close", os.O_RDONLY)
        self.cfg.set(section, "read", os.O_RDONLY)
        self.cfg.set(section, "reread", os.O_RDONLY)
        self.cfg.set(section, "write", os.O_CREAT | os.O_RDWR)
        self.cfg.set(section, "rewrite", os.O_CREAT | os.O_RDWR)
        self.cfg.set(section, "offsetread", os.O_RDONLY)
        self.cfg.set(section, "offsetwrite", os.O_CREAT | os.O_RDWR)
        self.cfg.set(section, "fread", 'r')
        self.cfg.set(section, "freread", 'r')
        self.cfg.set(section, "fwrite", 'w')
        self.cfg.set(section, "frewrite", 'w')
        
        # mode
        section = "mode"
        self.cfg.add_section(section)
        self.cfg.set(section, "creat", 0600);
        self.cfg.set(section, "access", os.F_OK);
        self.cfg.set(section, "chmod", stat.S_IEXEC);
        self.cfg.set(section, "write", 0600);
        self.cfg.set(section, "rewrite", 0600);

        with open(filename, "wb") as configfile:
            self.cfg.write(configfile)
        
    #
    # Parse/Store options from/to command line
    #
    def parse_argv(self, argv):
        return

if __name__ == "__main__":
    c = ParamarkOptions()
    c.store_conf()
