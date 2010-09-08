#############################################################################
# ParaMark: A Parallel/Distributed File Systems Benchmark
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
# modules/verbose.py
# Vebose Management
#

import sys

__all__ = ["message", "warning", "verbose", "fatal"]

verbosity = 0

def message(s):
    sys.stdout.flush()
    sys.stdout.write("%s\n" % s)

def warning(s):
    sys.stdout.flush()
    sys.stdout.write("Warning: %s\n" % s)

def debug(s):
    sys.stdout.write("Debug: %s\n" % s)

def verbose(s, level=0):
    if verbosity >= level:
        sys.stdout.flush()
        sys.stdout.write("%s\n" % s)

def fatal(s, ret=1):
    sys.stdout.flush()
    sys.stderr.write("Fatal: %s\n" % s)
    sys.exit(ret)
