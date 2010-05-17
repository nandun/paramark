#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
#############################################################################

__all__ = ["OPTYPE_META", "OPTYPE_IO", "FSOP_META", "FSOP_IO"]

OPTYPE_META = 1
OPTYPE_IO = 0

FSOP_META = ["mkdir", "creat", "access", "open", "open_close", "stat_exist", 
    "stat_non", "utime", "chmod", "rename", "unlink", "rmdir"]

FSOP_IO = ["read", "reread", "write", "rewrite", "fread", "freread",
    "fwrite", "frewrite", "offsetread", "offsetwrite"]
