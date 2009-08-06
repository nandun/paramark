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

# GChart Class
# Module for generate Google Chart HTML code

import os
import sys

from common import *

# http://www.childoflight.org/mcc/colorcodeA.html
COLOR_RAINBOW = ["C91F16", "E68601", "CED500", "69B011", "088343", "1F9AD7",
"113279", "5B0B5A", "C2224A", # deepest series
"E7860B", "D8DB09", "69AC12", "098D3A", "159BD4", "0D3981", "51095A"]
COLOR_RAINBOW_TRANSPARENT = map(lambda x:x+"F0", COLOR_RAINBOW)

__all__ = ["BarChart"]

class GoogleChart:
    """
    Basic class of all chart
    Input: parameters and data of chart
    Output: url string of Google Charts
    """
    def __init__(self):
        # chart settings
        self.name = "Google Chart"
        self.types = []
        self.dataformats = ["t", "s", "e"]
        self.dataformat = "t"
        self.dataseparator = ","
        self.dataseriesseparator = "|"
        self.dataseries = []
        self.datasetrangeseparator = ","
        self.labelseparator = "|"
        self.labelseriesseparator = "|"
        self.labelseries = []
        self.axistypes = ["x", "t", "y", "r"]
        self.axistypeseparator = ","
        self.axisranges = []
        self.axisrangeseparator = "|"

        # url strings requests
        self.reqs = {}
        # set default value
        self.reqs["chs"] = "800x370"

        # error message
        self.errstrs = [] # if empty, then no error

        # constants
        self.requrl = "http://chart.apis.google.com/chart?" # request url
        self.maxarea = 300000   # limit of chart area
        self.maxlength = 1000   # limit of width/height
    
    # chart generation routines
    def setsize(self, width=800, height=370):
        if width > self.maxlength:
            self.errstrs.append("Oops, chart width %d exceeded limit %d.\n"
                % (width, self.maxlength))
        if height > self.maxlength:
            self.errstrs.append("Oops, chart height %d exceeded limit %d.\n"
                % (height, self.maxlength))
        if width * height > self.maxarea:
            self.errstrs.append("Oops, chart area %d exceeded limit %d.\n"
                % (width * height, self.maxarea))
        self.reqs["chs"] = "%dx%d" % (width, height)

    def settype(self, type):
        if type not in self.types:
            self.errstrs.append("Oops, %s does not have type %d.\n"
                % (self.name, type))
        self.reqs["cht"] = "%s" % type

    def setdataformat(self, format):
        if format not in self.dataformats:
            self.errstrs.append("Oops, data format %s is not available.\n"
                % format)
        if format == "t":
            self.dataseriesseparator = "|"
        elif format in ["s", "e"]:
            self.dataseriesseparator = ","
        self.dataformat = format
    
    def adddataseries(self, series, data):
        self.dataseries.insert(series, data)
    
    def setdata(self, listofdata=None):
        if listofdata: self.dataseries = listofdata
        
        datamin = INTEGER_MAX
        datamax = INTEGER_MIN
        dstrlist = []
        for series in self.dataseries:
            dstrlist.append(self.dataseparator.join(list_tostring(series)))
            datamin = min(datamin, min(series))
            datamax = max(datamax, max(series))
        self.reqs["chd"] = "%s:%s" % (self.dataformat, 
            self.dataseriesseparator.join(dstrlist))

        return datamin, datamax

    def addlabelseries(self, series, label):
        self.labelseries.insert(series, label)
    
    def setlabels(self, labels=None):
        if labels: self.labelseries = labels
        
        serieslist = []
        for series in self.labelseries:
            serieslist.append(self.labelseparator.join(series))
        self.reqs["chl"] = self.labelseriesseparator.join(serieslist)
    
    def html(self, type="img"):
        """ return the html code of requested chart """
        # compose requests
        urlstr = self.requrl + "\n" + \
            "\n&amp;".join(map(lambda x:"%s=%s" % x, self.reqs.items()))

        if type == "img": return "<img src=\"%s\"/>" % urlstr
        else: return urlstr

class BarChart(GoogleChart):
    def __init__(self, opts=None, **kw):
        GoogleChart.__init__(self)

        self.name = "Bar Chart"
        self.types.extend(["bhs", "bvs", "bhg", "bvg"]) # bar chart types

        # bar chart specific parameters
        self.reqs["cht"] = "bvg"
        self.reqs["chbh"] = "a"
        # self.reqs["chxt"] = "y,x"   # axis type

        # set default settings
        self.setdataformat("t")

    def addaxisrange(self, type, start, end, interval):
        if type not in self.axistypes:
            self.errstrs.append("Oops, wrong axis type %s.\n" % type)
        self.axisranges.append((type, start, end, interval))

    def setaxisrange(self):
        chxr = []
        chxt = []
        index = 0
        for index in range(0, len(self.axisranges)):
            type, start, end, interval = self.axisranges[index]
            chxt.append(type)
            chxr.append("%d,%s,%s,%s" % (index, start, end, interval))
        self.reqs["chxt"] = self.axistypeseparator.join(chxt)
        self.reqs["chxr"] = self.axisrangeseparator.join(chxr)
    
    def setdata(self, listofdata=None):
        datamin, datamax = GoogleChart.setdata(self, listofdata)
        
        ystart, yend, yinterval = loose_ticks(datamin, datamax, 10)
        # need to explicitly set data range
        self.reqs["chds"] = self.datasetrangeseparator.join(list_tostring(
            [ystart, yend]))
        self.addaxisrange("y", ystart, yend, yinterval)
        self.setaxisrange()
