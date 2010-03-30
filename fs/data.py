#############################################################################
# ParaMark: A Benchmark for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
# Distributed under GNU General Public Licence version 3
#############################################################################

#
# fs/data.py
# File System Benchmark Data Persistence and Processing
#

import sqlite3
import cPickle
import ConfigParser

import numpy

from const import *

class Database:
    """Store/Retrieve benchmark results data"""
    def __init__(self, dbfile, initTables):
        # Constants
        self.FORMATS = {}
        self.FORMATS['runtime'] = [('item','TEXT'), ('value','TEXT')]
        self.FORMATS['conf'] = [('sec','TEXT'), ('opt','TEXT'), 
            ('val', 'TEXT')]
        self.FORMATS['rawdata'] = [('hostid','INTEGER'), ('pid','INTEGER'), 
            ('tid','INTEGER'), ('oper','TEXT'), ('optype', 'INTEGER'), 
            ('data','BLOB'), ('sync','REAL')]
        self.FORMATS['aggdata'] = [('hostid','INTEGER'), ('pid','INTEGER'),
            ('tid','INTEGER'), ('oper','TEXT'), ('optype', 'INTEGER'), 
            ('min','REAL'), ('max','REAL'), ('avg','REAL'), ('agg','REAL'), 
            ('std','REAL'), ('time', 'REAL')]
        
        self.FORMATS_LEN = {}
        for k, v in self.FORMATS.items():
            self.FORMATS_LEN[k] = len(self.FORMATS[k])

        self.dbfile = dbfile
        self.db = sqlite3.connect(dbfile)
        self.cur = self.db.cursor()
        self.tables = []    # all tables in database

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
    
    # Table
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

    def drop_tables(self, tables=None):
        if tables is None:
            tables = self.tables
        for table in tables:
            self.cur.execute("DROP TABLE IF EXISTS %s" % table)
            
    # Pickle
    def _obj2str(self, obj):
        return cPickle.dumps(obj)
    
    def _str2obj(self, objs):
        return cPickle.loads(str(objs))

    # Data Tables
    def _ins_rawdata(self, table, data):
        """Insert data to table in rawdata format"""
        assert len(data) == self.FORMATS_LEN['rawdata']
        self.cur.execute("INSERT INTO %s VALUES (?,?,?,?,?,?,?)" 
            % table, data)
    
    def _ins_aggdata(self, table, data):
        """Insert data to table in aggdata format"""
        assert len(data) == self.FORMATS_LEN['aggdata']
        self.cur.execute("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?)" 
            % table, data)
        
    def ins_runtime(self, runtimes, pickleValue=True, overwrite=True):
        """Save runtime variables into table runtime
        runtimes: dictionary of runtime key and values
        pickleValue: whether convert object to string
        """
        table = 'runtime'
        self.create_table(table, self.FORMATS[table], overwrite) 
        for k, v in runtimes.items():
            if pickleValue:
                v = self._obj2str(v)
            self.cur.execute("INSERT INTO %s VALUES (?,?)" % table, (k, v))
        self.db.commit()

    def ins_conf(self, filename, overwrite=True):
        table = 'conf'
        self.create_table(table, self.FORMATS[table], overwrite) 
        cfg = ConfigParser.ConfigParser()
        cfg.read([filename])
        for sec in cfg.sections():
            for opt, val in cfg.items(sec):
                self.cur.execute('INSERT INTO %s VALUES (?,?,?)' % table,
                    (sec, opt, val))
        
    def ins_rawdata(self, res, start, overwrite=True):
        """Save result data to table data
        runset:
        pickleValue:
        start: start time used to normalize data
        """
        table = 'data'
        self.create_table(table, self.FORMATS['rawdata'], overwrite) 
        for r in res:
            sync_prev_name, sync_prev_time = r.synctime.pop(0)
            for o in r.opset:
                sync_name, sync_time = r.synctime.pop(0)
                assert sync_name == o.name
                data = map(lambda (s,e):(s-start,e-start), o.res)
                self._ins_rawdata(table, (r.hid, r.pid, r.tid, 
                    o.name, o.type, self._obj2str(data), 
                    sync_time-sync_prev_time))
                sync_prev_name, sync_prev_time = sync_name, sync_time
    
    def _sel(self, one, table, columns, where, group):
        qstr = 'SELECT %s FROM %s' % (','.join(columns), table)
        if where is not None and len(where) > 0:
            wstr = ' AND '.join(['%s="%s"' % (k,v) for k,v in where.items()])
            qstr = '%s WHERE %s' % (qstr, wstr)
        if group is not None:
            qstr = '%s GROUP BY %s' (qstr, group)
        self.cur.execute(qstr)
        if one:
            res = self.cur.fetchone()
        else:
            res = self.cur.fetchall()
        if len(columns) == 1:
            return map(lambda (v,):v, res)
        else:
            return res
    
    def get_conf_val(self, sec, opt):
        """Get configuration value"""
        self.cur.execute('SELECT val FROM conf WHERE sec="%s" AND opt="%s"'
            % (sec, opt))
        res = self.cur.fetchone()
        if res is not None:
            return eval(res[0])
        return None

    def get_runtimes(self):
        self.cur.execute('SELECT item,value FROM runtime')
        return map(lambda (k,v):(k,self._str2obj(v)), self.cur.fetchall())
    
    def get_hosts(self):
        self.cur.execute('SELECT hostid FROM data GROUP BY hostid')
        return map(lambda (v,):v, self.cur.fetchall())

    def get_opers(self):
        self.cur.execute('SELECT oper FROM data GROUP BY oper')
        return map(lambda (v,):v, self.cur.fetchall())

    def agg_thread(self, overwrite=False):
        table = 'data0'
        src = 'data'
        self.create_table(table, self.FORMATS['aggdata'], overwrite)
        
        # TODO:
        # follow three variables should be retrieved accroding to oper
        # instead of global ones, but need to fix opts setting part
        opcnt = self.get_conf_val('global', 'opcnt')
        fsize = self.get_conf_val('global', 'fsize')
        bsize = self.get_conf_val('global', 'bsize')
        
        for hid,pid,tid,op,optype,dat,sync in self._sel(False, src, 
            ['hostid', 'pid', 'tid', 'oper', 'optype', 'data', 'sync'], 
            None, None):
            dat = map(lambda (s,e):(e-s), self._str2obj(dat))
            if optype == OPTYPE_META:
                thlist = map(lambda t:1.0/t, dat)
                thagg = opcnt / numpy.sum(dat)
            elif optype == OPTYPE_IO:
                thlist = map(lambda t:bsize/t, dat)
                thagg = fsize / numpy.sum(dat)
            thmin = numpy.min(thlist)
            thmax = numpy.max(thlist)
            thavg = numpy.average(thlist)
            thstd = numpy.std(thlist)
            self._ins_aggdata(table, (hid,pid,tid,op,optype,thmin,thmax,thavg,
                thagg,thstd,sync))
    
    def _get_sync_time(self, listofsync):
        # TODO
        # avg?
        #return numpy.max(listofsync)
        return numpy.average(listofsync)

    def agg_host(self, overwrite=False):
        src = 'data0'
        table = 'data1'
        self.create_table(table, self.FORMATS['aggdata'], overwrite)
        
        # TODO:
        # follow three variables should be retrieved accroding to oper
        # instead of global ones, but need to fix opts setting part
        opcnt = self.get_conf_val('global', 'opcnt')
        fsize = self.get_conf_val('global', 'fsize')
        bsize = self.get_conf_val('global', 'bsize')
        
        for h in self.get_hosts():
            for o in self.get_opers():
                self.cur.execute("SELECT optype,agg,time FROM %s "
                    "WHERE hostid='%s' AND oper='%s'" % (src, h, o))
                optype, thlist, tmlist = zip(*self.cur.fetchall())
                optype = optype[0]
                n_threads = len(thlist)
                tm = self._get_sync_time(tmlist)
                if optype == OPTYPE_META:
                    thagg = n_threads * opcnt / tm
                elif optype == OPTYPE_IO:
                    thagg = n_threads * fsize / tm
                thmin = numpy.min(thlist)
                thmax = numpy.max(thlist)
                thavg = numpy.average(thlist)
                thstd = numpy.std(thlist)
                self._ins_aggdata(table, \
                    (h,-1,n_threads,o,optype,thmin,thmax,thavg,thagg,thstd,tm))
    
    def agg_all(self, overwrite=False):
        src = 'data1'
        table = 'data2'
        self.create_table(table, self.FORMATS['aggdata'], overwrite)
        
        # TODO:
        # follow three variables should be retrieved accroding to oper
        # instead of global ones, but need to fix opts setting part
        opcnt = self.get_conf_val('global', 'opcnt')
        fsize = self.get_conf_val('global', 'fsize')
        bsize = self.get_conf_val('global', 'bsize')
        
        for o in self.get_opers():
            self.cur.execute("SELECT tid,optype,agg,time FROM %s "
                "WHERE oper='%s'" % (src, o))
            tidlist, optype, thlist, tmlist = zip(*self.cur.fetchall())
            optype = optype[0]
            n_threads = int(numpy.sum(tidlist))
            tm = self._get_sync_time(tmlist)
            if optype == OPTYPE_META:
                thagg = n_threads * opcnt / tm
            elif optype == OPTYPE_IO:
                thagg = n_threads * fsize / tm
            thmin = numpy.min(thlist)
            thmax = numpy.max(thlist)
            thavg = numpy.average(thlist)
            thstd = numpy.std(thlist)
            self._ins_aggdata(table,
                (-1,-1,n_threads,o,optype,thmin,thmax,thavg,thagg,thstd,tm))

    def get_stats(self, table, columns, where):
        return self._sel(False, table, columns, where, None)

    def get_stat_all(self):
        self.cur.execute("SELECT tid,oper,optype,min,max,avg,agg,std"
            " FROM data2")
        return self.cur.fetchall()
        
###### OLD VERSION ####################################################
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
    
    def io_get_data(self, **where):
        qstr = "SELECT data FROM io %s"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):self._str2obj(v), self.cur.fetchall())
