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

import Gnuplot

class Plot:
    def __init__(self):
        self.c = Gnuplot.Gnuplot()
        self.terminal = "png"
    
    def points_chart(self, xdata, ydata, prefix="points_chart",
        title="points_chart"):
        filepath = "%s.%s" % (prefix, self.terminal)
        self.c.reset()
        self.c("set terminal %s" % self.terminal)
        self.c("set output %s" % filepath)
        self.c("set data style points")
        plotdata = zip(xdata, ydata)
        self.c.plot(plotdata)

        return filepath

    def line_chart(self, xdata, ydata, filename="line_chart.png",
        title="line_chart"):
        assert len(xdata) == len(ydata)
        self.c.reset()
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s'" % filename)
        self.c("set data style linespoints")
        plotdata = zip(xdata, ydata)
        self.c.plot(plotdata)
