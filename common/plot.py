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
import matplotlib
matplotlib.use("Cairo")
import matplotlib.pyplot as pyplot

class Plot:
    def __init__(self):
        self.c = Gnuplot.Gnuplot()
        self.terminal = "png"
    
    def points_chart(self, xdata, ydata, prefix="points_chart",
        title="points_chart", xlabel="x label", ylabel="y label"):
        filepath = "%s.%s" % (prefix, self.terminal)
        self.c.reset()
        self.c.title(title)
        self.c.xlabel(xlabel)
        self.c.ylabel(ylabel)
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s'" % filepath)
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

    def bars_chart(self, xdata, ydata, filename):
        self.c.reset()
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s'" % filename)
        self.c("set data style boxes")
        plotdata = zip(xdata, ydata)
        self.c.plot(plotdata)

class Pyplot:
    def bar(self, path, data, yerr=[], xticks=[], 
        title="", xlabel="", ylabel=""):
        pyplot.clf()
        pyplot.title(title)
        pyplot.xlabel(xlabel)
        pyplot.ylabel(ylabel)
        ind = map(lambda x:x+0.1, range(0, len(data)))
        xtickind = map(lambda x:x+0.5, range(0, len(data)))
        pyplot.xticks(xtickind, xticks, rotation="90")
        pyplot.bar(ind, data, color='y', yerr=yerr)
        F = pyplot.gcf()
        F.set_size_inches((15, 10))
        pyplot.savefig(path)
