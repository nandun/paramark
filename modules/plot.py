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
# plot.py
# Data plotting
#

import sys
from verbose import *

class GnuPlot:
    def __init__(self, path):
        try:
            import Gnuplot
        except ImportError:
            message(
"""Module Gnuplot not found, please run "sudo apt-get install python-gnuplot"
or refer to http://gnuplot-py.sourceforge.net/.""")
            raise ImportError
            
        self.p = Gnuplot.Gnuplot()
        self.path = path
    
    def impulse_chart(self, data, name="bar_chart", 
        title="impulse_chart", xlabel="x_label", ylabel="y_label",
        xmin=None, xmax=None, ymin=None, ymax=None,
        xlog=False, ylog=False):
        self.p.reset()
        self.p("set terminal png")
        self.p("set output '%s/%s'" % (self.path, name))
        self.p.title(title)
        self.p("set xlabel '%s'" % xlabel)
        self.p("set ylabel '%s'" % ylabel)
        if xlog: self.p("set logscale x")
        if ylog: self.p("set logscale y")
        if xmin is None: xmin = 0
        if xmax is None: xmax = len(data)
        # let gnuplot decide range when log scale is set
        if not xlog: self.p("set xrange [%d:%d]" % (xmin, xmax))
        if ymin is not None and ymax is not None and not ylog:
            self.p("set yrange [%d:%d]" % (ymin, ymax))
        self.p("set data style impulses")
        self.p.plot(data)
