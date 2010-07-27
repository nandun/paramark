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

class Verbose():
    def __init__(self):
        self.level = 0

    def verbose(self, s, level=1):
        if self.level >= level:
            sys.stdout.write("%s\n" % s)
            sys.stdout.flush()

    def warning(self, s):
        sys.stdout.write("%s\n" % s)
        sys.stdout.flush()

    def error(self, s):
        sys.stderr.write("%s\n" % s)

vbs = Verbose()
