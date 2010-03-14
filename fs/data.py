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
# fsdata.py
# File System Benchmark Data Persistence and Processing
#

import sqlite3
import cPickle

import numpy

from bench import OPTYPE_META, OPTYPE_IO, FSOP_META, FSOP_IO

class Database:
    """Store/Retrieve benchmark results data"""
    def __init__(self, dbfile, initTables):
        self.dbfile = dbfile
        self.db = sqlite3.connect(dbfile)
        self.cur = self.db.cursor()
        self.tables = []    # all tables in database

        if initTables: self.init_tables()

    def close(self):
        self.db.commit()
        self.db.close()
        self.db = None

    def init_tables(self):
        """Create tables for database
        NOTE: existing tables will be dropped"""
        # runtime
        self.cur.execute("DROP TABLE IF EXISTS runtime")
        self.cur.execute("CREATE TABLE IF NOT EXISTS runtime"
            "(item TEXT, value TEXT)")
        self.tables.append("runtime")

        # tests table
        self.cur.execute("DROP TABLE IF EXISTS tests")
        self.cur.execute("CREATE TABLE IF NOT EXISTS tests"
            "(testid INTEGER, opcnt INTEGER, factor INTEGER, fsize INTEGER,"
            "bsize INTEGER)")
        self.tables.append("tests")
        
        # data tables
        self.cur.execute("DROP TABLE IF EXISTS meta")
        self.cur.execute("CREATE TABLE IF NOT EXISTS meta"
            "(hostid INTEGER, pid INTEGER, tid INTEGER, testid INTEGER,"
            "oper TEXT, data BLOB)")
        self.tables.append("meta")
        self.cur.execute("DROP TABLE IF EXISTS io")
        self.cur.execute("CREATE TABLE IF NOT EXISTS io"
            "(hostid INTEGER, pid INTEGER, tid INTEGER, testid INTEGER,"
            "oper TEXT, data BLOB)")
        self.tables.append("io")

    def drop_tables(self):
        for table in self.tables:
            self.cur.execute("DROP TABLE IF EXISTS %s" % table)
            
    # Pickle
    def obj2str(self, obj):
        return cPickle.dumps(obj)
    
    def str2obj(self, objs):
        return cPickle.loads(str(objs))

    # Table Operations
    def runtime_ins(self, runtimes, pickleValue=True):
        """Save runtime variables into table runtime
        runtimes: dictionary of runtime key and values
        pickleValue: whether convert object to string
        """
        for item, value in runtimes.items():
            if pickleValue: value = self.obj2str(value)
            self.cur.execute("INSERT INTO runtime VALUES (?,?)", (item, value))
        self.db.commit()
    
    def runtime_sel(self, fields="*", pickleValue=True):
        self.cur.execute("SELECT %s FROM runtime" % fields)
        if pickleValue:
            return map(lambda (i,v):(i,self.str2obj(v)), self.cur.fetchall())
        else:
            return self.cur.fetchall()

    def data_ins(self, threads, start, pickleValue=True):
        """Save result data to table data
        runset:
        pickleValue:
        start: start time used to normalize data
        """
        for t in threads:
            for o in t.opset:
                data = map(lambda (s,e):(s-start,e-start), o.res)
                if pickleValue: data = self.obj2str(data)
                # TODO: test id
                if o.type == OPTYPE_META:
                    self.cur.execute("INSERT INTO meta VALUES (?,?,?,?,?,?)", 
                        (t.hostid, t.pid, t.tid, t.testid, o.name, data))
                elif o.type == OPTYPE_IO:
                    self.cur.execute("INSERT INTO io VALUES (?,?,?,?,?,?)", 
                        (t.hostid, t.pid, t.tid, t.testid, o.name, data))

    def meta_sel(self, columns, **where):
        qstr = "SELECT %s FROM meta" % columns
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]),
            where.keys()))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        res = self.cur.fetchall()
        res = map(lambda (o,i,d):(o,i,self.str2obj(d)), res)
        return res
    
    def meta_get_testids(self):
        qstr = "SELECT testid FROM meta GROUP BY testid"
        self.cur.execute(qstr)
        return map(lambda (v,):v, self.cur.fetchall())

    def meta_get_opers(self, **where):
        qstr = "SELECT oper FROM meta %s GROUP BY oper"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):str(v), self.cur.fetchall())

    def meta_get_hosts(self):
        qstr = "SELECT hostid FROM meta GROUP BY hostid"
        self.cur.execute(qstr)
        return map(lambda (v,):v, self.cur.fetchall())
    
    def meta_get_data(self, **where):
        qstr = "SELECT data FROM meta %s"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):self.str2obj(v), self.cur.fetchall())

    def meta_stats_by_host(self, oper, host):
        qstr = "SELECT data FROM meta WHERE oper='%s' AND hostid='%s'" \
            % (oper, host)
        self.cur.execute(qstr)
        thputlist = []
        for dat in map(lambda (v,):self.str2obj(v), self.cur.fetchall()):
            thputlist.append(len(dat)/numpy.sum(map(lambda (s, e):e-s, dat)))
        thputavg = numpy.average(thputlist)
        thputmin = numpy.min(thputlist)
        thputmax = numpy.max(thputlist)
        thputstd = numpy.std(thputlist)
        return thputavg, thputmin, thputmax, thputstd, thputlist
    
    def meta_get_tid_and_data(self, **where):
        qstr = "SELECT tid,data FROM meta %s"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,d):(v,self.str2obj(d)), self.cur.fetchall())
    
    def io_get_testids(self):
        qstr = "SELECT testid FROM io GROUP BY testid"
        self.cur.execute(qstr)
        return map(lambda (v,):v, self.cur.fetchall())
    
    def io_get_opers(self, **where):
        qstr = "SELECT oper FROM io %s GROUP BY oper"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):str(v), self.cur.fetchall())
    
    def io_get_hosts(self):
        qstr = "SELECT hostid FROM io GROUP BY hostid"
        self.cur.execute(qstr)
        return map(lambda (v,):v, self.cur.fetchall())
    
    def io_stats_by_host(self, oper, host):
        qstr = "SELECT data FROM io WHERE oper='%s' AND hostid='%s'" \
            % (oper, host)
        self.cur.execute(qstr)
        thputlist = []
        for dat in map(lambda (v,):self.str2obj(v), self.cur.fetchall()):
            print oper, host, dat
        return None
    
    def io_get_data(self, **where):
        qstr = "SELECT data FROM io %s"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):self.str2obj(v), self.cur.fetchall())
