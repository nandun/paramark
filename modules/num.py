#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
#############################################################################

#
# modules/num.py
# numerical functions
#

import __builtin__
import math

HAVE_NUMPY = False
try:
    import numpy
    HAVE_NUMPY = True
except ImportError:
    HAVE_NUMPY = False

def num_average(alist):
    return sum(alist)/len(alist)

def num_std(alist):
    avg = num_average(alist)
    total = 0.0
    for x in alist:
        total += math.pow((x-avg), 2)
    return math.sqrt(total/len(alist))

if HAVE_NUMPY:
    sum = numpy.sum
    average = numpy.average
    min = numpy.min
    max = numpy.max
    std = numpy.std
else:
    sum = __builtin__.sum
    average = num_average
    min = __builtin__.min
    max = __builtin__.max
    std = num_std

