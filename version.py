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

#
# ParaMark Information
#

PARAMARK_VERSION = 0.4
PARAMARK_DATE = "2009-12-22"
PARAMARK_AUTHOR = "Nan Dun"
PARAMARK_BUG_REPORT = "dunnan@yl.is.s.u-tokyo.ac.jp"
PARAMARK_WEB = "http://paramark.googlecode.com/"
PARAMARK_LICENCE = "GNU General Public License"
PARAMARK_LICENCE_VERSION = 3

PARAMARK_VERSION_STRING = """\
ParaMark: High Fidelity Parallel File System Benchmark
Version: %s, build %s
Author: %s <%s>
Web: %s

This program is free software: you can redistribute it and/or modify
it under the terms of the %s as published by
the Free Software Foundation, either version %s of the License, or
(at your option) any later version.
""" % (PARAMARK_VERSION, PARAMARK_DATE, PARAMARK_AUTHOR, PARAMARK_BUG_REPORT,
PARAMARK_WEB, PARAMARK_LICENCE, PARAMARK_LICENCE_VERSION)
