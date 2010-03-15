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
# fs/data.py
# File System Benchmark Data Persistence and Processing
#

import sqlite3
import cPickle

import numpy

from bench import OPTYPE_META, OPTYPE_IO, FSOP_META, FSOP_IO

class Database:
    """Store/Retrieve benchmark results data"""
    def __init__(self, dbfile, initTables):
        # Constants
        self.FORMATS = {}
        self.FORMATS_LEN = {}
        self.FORMATS['runtime'] = [('item','TEXT'), ('value','TEXT')]
        self.FORMATS_LEN['runtime'] = len(self.FORMATS['runtime'])
        # raw data list of benchmark results
        self.FORMATS['rawdata'] = [('hostid','INTEGER'), ('pid','INTEGER'), 
            ('tid','INTEGER'), ('oper','TEXT'), ('args', 'BLOB'), 
            ('data','BLOB'), ('sync','REAL')]
        self.FORMATS_LEN['rawdata'] = len(self.FORMATS['rawdata'])
        # aggregated results from group of threads
        self.FORMATS['aggdata'] = [('hostid','INTEGER'), ('pid','INTEGER'),
            ('tid','INTEGER'), ('oper','TEXT'), ('min','REAL'), 
            ('max','REAL'), ('avg','REAL'), ('agg','REAL'), ('std','REAL'), 
            ('time', 'REAL')]
        self.FORMATS_LEN['aggdata'] = len(self.FORMATS['aggdata'])

        self.dbfile = dbfile
        self.db = sqlite3.connect(dbfile)
        self.cur = self.db.cursor()
        self.tables = []    # all tables in database

        if initTables:
            self.init_tables()

    def __del__(self):
        """In case user forget to flush/close database"""
        if self.db is not None:
            self.db.commit()
            self.db.close()

    def close(self):
        """Flush and close database"""
        self.db.commit()
        self.db.close()
        self.db = None

    def create_table(self, name, format, drop=False):
        """Create a table given a table name and format
        Drop existing table if the drop option is True
        name: table name
        format: a list describe the field and its type
        drop: True/False"""

        if drop:
            self.cur.execute("DROP TABLE IF EXISTS %s" % name)
        formatstr =", ".join(["%s %s" % item for item in format]) 
        self.cur.execute("CREATE TABLE IF NOT EXISTS %s (%s)" 
            % (name, formatstr))
        self.tables.append("%s" % name)

    def init_tables(self):
        """Create tables for database
        NOTE: existing tables will be dropped"""
        
        self.create_table("runtime", self.FORMATS["runtime"], True) 
        self.create_table("meta", self.FORMATS["rawdata"], True)
        self.create_table("io", self.FORMATS["rawdata"], True)

    def drop_tables(self):
        for table in self.tables:
            self.cur.execute("DROP TABLE IF EXISTS %s" % table)
            
    # Pickle
    def _obj2str(self, obj):
        return cPickle.dumps(obj)
    
    def _str2obj(self, objs):
        return cPickle.loads(str(objs))

    # Table Operations
    def _ins_rawdata(self, table, data):
        """Insert data to table in rawdata format"""
        assert len(data) == self.FORMATS_LEN['rawdata']
        self.cur.execute("INSERT INTO %s VALUES (?,?,?,?,?,?,?)" % table, data)
    
    def _ins_aggdata(self, table, data):
        """Insert data to table in aggdata format"""
        assert len(data) == self.FORMATS_LEN['aggdata']
        self.cur.execute("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?)" % table,
            data)
        
    def runtime_ins(self, runtimes, pickleValue=True):
        """Save runtime variables into table runtime
        runtimes: dictionary of runtime key and values
        pickleValue: whether convert object to string
        """
        for item, value in runtimes.items():
            if pickleValue: value = self._obj2str(value)
            self.cur.execute("INSERT INTO runtime VALUES (?,?)", (item, value))
        self.db.commit()
    
    def runtime_sel(self, fields="*", pickleValue=True):
        self.cur.execute("SELECT %s FROM runtime" % fields)
        if pickleValue:
            return map(lambda (i,v):(i,self._str2obj(v)), self.cur.fetchall())
        else:
            return self.cur.fetchall()
    
    def ins_rawdata(self, threads, start, pickleValue=True):
        """Save result data to table data
        runset:
        pickleValue:
        start: start time used to normalize data
        """
        for t in threads:
            sync_prev_name, sync_prev_time = t.synctime.pop(0)
            for o in t.opset:
                sync_name, sync_time = t.synctime.pop(0)
                assert sync_name == o.name
                data = map(lambda (s,e):(s-start,e-start), o.res)
                if o.type == OPTYPE_META:
                    table = "meta"
                    args = {'opcnt':o.opcnt}
                elif o.type == OPTYPE_IO:
                    table = "io"
                    args = {'fsize':o.fsize, 'blksize':o.blksize}
                if pickleValue:
                    data = self._obj2str(data)
                    args = self._obj2str(args)
                self._ins_rawdata(table, (t.hostid, t.pid, t.tid, o.name, 
                        args, data, sync_time-sync_prev_time))
                sync_prev_name, sync_prev_time = sync_name, sync_time
    
    def meta_sel(self, columns, **where):
        qstr = "SELECT %s FROM meta" % columns
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]),
            where.keys()))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        res = self.cur.fetchall()
        res = map(lambda (o,i,d):(o,i,self._str2obj(d)), res)
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
        return map(lambda (v,):self._str2obj(v), self.cur.fetchall())

    def meta_stats_by_host(self, oper, host):
        qstr = "SELECT data FROM meta WHERE oper='%s' AND hostid='%s'" \
            % (oper, host)
        self.cur.execute(qstr)
        thputlist = []
        for dat in map(lambda (v,):self._str2obj(v), self.cur.fetchall()):
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
        return map(lambda (v,d):(v,self._str2obj(d)), self.cur.fetchall())
    
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
        for dat in map(lambda (v,):self._str2obj(v), self.cur.fetchall()):
            thputlist.append(len(dat)/numpy.sum(map(lambda (s, e):e-s, dat)))
        thputavg = numpy.average(thputlist)
        thputmin = numpy.min(thputlist)
        thputmax = numpy.max(thputlist)
        thputstd = numpy.std(thputlist)
        return thputavg, thputmin, thputmax, thputstd, thputlist
        #for dat in map(lambda (v,):self._str2obj(v), self.cur.fetchall()):
        #    print oper, host, dat
    
    def io_get_data(self, **where):
        qstr = "SELECT data FROM io %s"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):self._str2obj(v), self.cur.fetchall())

    # Statistic and Aggregation
    def _aggregate_0(self, overwrite=False):
        """Thread-level statistics"""
        self._aggregate_meta_0(overwrite)
        self._aggregate_io_0(overwrite)

    def _aggregate_meta_0(self, overwrite=False):
        self.cur.execute("SELECT oper,data FROM meta")
        for op, dat in self.cur.fetchall():
            print op, dat
    
    def _aggregate_io_0(self, overwrite=False):
        """Thread-level I/O statistics"""
        table = 'io0'
        self.create_table(table, self.FORMATS['aggdata'], overwrite)
        self.cur.execute('SELECT hostid,pid,tid,oper,args,data,sync FROM io')
        for hostid,pid,tid,oper,args,dat,sync in self.cur.fetchall():
            args = self._str2obj(args)
            dat = map(lambda (s,e):(e-s), self._str2obj(dat))
            thlist = map(lambda x:args['blksize']/x, dat)
            thmin = numpy.min(thlist)
            thmax = numpy.max(thlist)
            thavg = numpy.average(thlist)
            thagg = args['fsize']/numpy.sum(dat)
            thstd = numpy.std(thlist)
            self._ins_aggdata(table, (hostid,pid,tid,oper,thmin,thmax,thavg,
                thagg,thstd,sync))
    
    def aggregate_io_by_host(self, overwrite=False):
        """Host-level I/O statistics"""
        level = 1
        table = 'io%d' % level
        src = 'io%d' % (level - 1)
        self.create_table(table, self.FORMATS['aggdata'], overwrite)
        self.cur.execute('SELECT args FROM io')
        args = self._str2obj(self.cur.fetchone()[0])
        
        for h in self.io_get_hosts():
            for o in self.io_get_opers(hostid=h):
                self.cur.execute("SELECT agg,time FROM %s "
                    "WHERE hostid='%s' AND oper='%s'" % (src, h, o))
                thlist, tmlist = zip(*self.cur.fetchall())
                n_threads = len(thlist)
                tm = numpy.max(tmlist) # TODO: use max here
                thmin = numpy.min(thlist)
                thmax = numpy.max(thlist)
                thavg = numpy.average(thlist)
                thagg = n_threads * args['fsize'] / tm
                thstd = numpy.std(thlist)
                self._ins_aggdata(table, \
                    (h,-1,n_threads,o,thmin,thmax,thavg,thagg,thstd,tm))

    def aggregate_io_by_all(self, overwrite=False):
        level = 2
        table = 'io%d' % level
        src = 'io%d' % (level - 1)
        self.create_table(table, self.FORMATS['aggdata'], overwrite)
        self.cur.execute('SELECT args FROM io')
        args = self._str2obj(self.cur.fetchone()[0])
        
        for o in self.io_get_opers():
            self.cur.execute("SELECT tid,agg,time FROM %s "
                "WHERE oper='%s'" % (src, o))
            tidlist, thlist, tmlist = zip(*self.cur.fetchall())
            n_threads = 0
            for i in tidlist:
                n_threads += i
            tm = numpy.max(tmlist) # TODO: use max here
            thmin = numpy.min(thlist)
            thmax = numpy.max(thlist)
            thavg = numpy.average(thlist)
            thagg = n_threads * args['fsize'] / tm
            thstd = numpy.std(thlist)
            self._ins_aggdata(table, 
                (-1,-1,n_threads,o,thmin,thmax,thavg,thagg,thstd,tm))
