#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
#############################################################################

#
# modules/gxp.py
# GXP related routines
#

import sys
import os
import socket
import fcntl

def ws(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def es(s):
    sys.stderr.write(s)
    sys.stderr.flush()

def set_close_on_exec():
    try:
        fd_3 = fcntl.fcntl(3, fcntl.F_GETFD)
        fd_4 = fcntl.fcntl(4, fcntl.F_GETFD)
    except IOError:
        fd_3 = fcntl.FD_CLOEXEC
        fd_4 = fcntl.FD_CLOEXEC
    fd_3 = fd_3 | fcntl.FD_CLOEXEC
    fd_4 = fd_4 | fcntl.FD_CLOEXEC
    fcntl.fcntl(3, fcntl.F_SETFD, fd_3)
    fcntl.fcntl(4, fcntl.F_SETFD, fd_4)

def get_rank():
    return int(os.environ.get("GXP_EXEC_IDX", "0"))

def get_size():
    return int(os.environ.get("GXP_NUM_EXECS", "1"))

class Host:
    def __init__(self, h, f, i, idx):
        self.h = h      # hostname
        self.f = f      # fqdn
        self.i = i      # ip address
        self.idx = idx  # GXP_EXEC_IDX
    
    def __repr__(self):
        return ("Host(%(h)r,%(f)r,%(i)r,%(idx)r)" % self.__dict__)

def get_my_host():
    h = socket.gethostname()
    f = socket.getfqdn()
    i = socket.gethostbyname(f)
    idx = get_rank()
    return Host(h, f, i, idx)

def get_all_hosts(wp, fp):
    wp.write("%r\n" % get_my_host())
    wp.flush()
    hosts = []
    for i in range(get_size()):
        line = fp.readline()
        assert line != ""
        host = eval(line.strip())
        hosts.append((host.idx, host))
    hosts.sort()
    hosts_list = map(lambda (idx,host): host, hosts)
    return hosts_list

def broadcast(wp, msg):
    wp.write(msg)
    wp.write('\n')
    wp.flush()

def receive(rp):
    msg = rp.read()
    assert msg != ""
    return msg.strip()
