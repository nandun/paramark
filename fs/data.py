#############################################################################
# ParaMark: Benchmarking Suite for Parallel/Distributed Systems
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>

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
# Benchmark Data Persistence and Retrieving
#

import sqlite3
import cPickle
import ConfigParser

from modules import num
import oper

class Database:
    """Store/Retrieve benchmark results data"""
    def __init__(self, path):
        # Constants
        self.FORMATS = {}
        self.FORMATS['runtime'] = [('item','TEXT'), ('value','TEXT')]
        self.FORMATS['conf'] = [('sec','TEXT'), ('opt','TEXT'), 
            ('val', 'TEXT')]
        self.FORMATS['io'] = [('hid','INTEGER'), ('pid','INTEGER'),
            ('tid','INTEGER'), ('fsize', 'INTEGER'), ('bsize', 'INTEGER'),
            ('elapsed', 'BLOB'), ('sync', 'REAL'),
            ('agg', 'REAL'), ('aggnoclose', 'REAL'),
            ('opavg', 'REAL'), ('opmin', 'REAL'), ('opmax', 'REAL'),
            ('opstd', 'REAL')]
        self.FORMATS['meta'] = [('hid','INTEGER'), ('pid','INTEGER'),
            ('tid','INTEGER'), ('opcnt', 'INTEGER'), ('factor', 'INTEGER'),
            ('elapsed', 'BLOB'), ('sync', 'REAL'),
            ('agg', 'REAL'), ('opavg', 'REAL'),
            ('opmin', 'REAL'), ('opmax', 'REAL'), ('opstd', 'REAL')]
        self.FORMATS['aggdata'] = [('hostid','INTEGER'), ('pid','INTEGER'),
            ('tid','INTEGER'), ('oper','TEXT'), ('optype', 'INTEGER'), 
            ('min','REAL'), ('max','REAL'), ('avg','REAL'), ('agg','REAL'), 
            ('std','REAL'), ('time', 'REAL')]
        
        self.FORMATS_LEN = {}
        for k, v in self.FORMATS.items():
            self.FORMATS_LEN[k] = len(self.FORMATS[k])
        
        sqlite3.register_converter("BLOB", lambda s:cPickle.loads(str(s)))
        sqlite3.register_adapter(list, cPickle.dumps)
        sqlite3.register_adapter(dict, cPickle.dumps)
        self.db = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
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
        if drop: self.cur.execute("DROP TABLE IF EXISTS %s" % name)
        
        if name in self.tables: return
        
        formatstr =", ".join(["%s %s" % item for item in format]) 
        self.cur.execute("CREATE TABLE IF NOT EXISTS %s (%s)" 
            % (name, formatstr))
        self.tables.append("%s" % name)

    def drop_tables(self, tables=None):
        if tables is None:
            tables = self.tables
        for table in tables:
            self.cur.execute("DROP TABLE IF EXISTS %s" % table)

    def get_tables(self):
        self.cur.execute("SELECT name FROM sqlite_master where type='table'")
        return map(lambda (v,):str(v), self.cur.fetchall())
            
    # Data Tables
    def _ins_aggdata(self, table, data):
        """Insert data to table in aggdata format"""
        assert len(data) == self.FORMATS_LEN['aggdata']
        self.cur.execute("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?)" 
            % table, data)
        
    def insert_runtime(self, runtimes, overwrite=True):
        """Save runtime variables into table runtime
        runtimes: dictionary of runtime key and values
        pickleValue: whether convert object to string
        """
        table = 'runtime'
        self.create_table(table, self.FORMATS[table], overwrite) 
        for k, v in runtimes.items():
            self.cur.execute("INSERT INTO %s VALUES (?,?)" % table, (k, v))
        self.db.commit()

    def insert_conf(self, filename, overwrite=True):
        table = 'conf'
        self.create_table(table, self.FORMATS[table], overwrite) 
        cfg = ConfigParser.ConfigParser()
        cfg.read([filename])
        for sec in cfg.sections():
            for opt, val in cfg.items(sec):
                self.cur.execute('INSERT INTO %s VALUES (?,?,?)' % table,
                    (sec, opt, val))

    def insert_rawdata(self, res, overwrite=False):
        """
        Insert raw data for the series of operation in each *thread*
        """
        for o in res.opset:
            if oper.optype(o["name"]) == oper.TYPE_META:
                # Aggregated throughput
                total_elapsed = num.sum(o["elapsed"])
                agg = o["opcnt"] / total_elapsed # ops/sec
                
                # Per-operation throughput
                tlist = map(lambda e:1/e, o["elapsed"])
                opavg = num.average(tlist)
                opmin = num.min(tlist)
                opmax = num.max(tlist)
                opstd = num.std(tlist)

                self.create_table(o["name"], self.FORMATS["meta"], overwrite)
                self.cur.execute(
                    "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
                    % o["name"], (res.hid, res.pid, res.tid, o["opcnt"],
                      o["factor"], o["elapsed"], o['synctime'],
                      agg, opavg, opmin, opmax, opstd))

            elif oper.optype(o["name"]) == oper.TYPE_IO:
                # Aggregated throughput
                total_elapsed = num.sum(o["elapsed"])
                agg = o["fsize"] / total_elapsed # KB/sec
                aggnoclose = o["fsize"] / (total_elapsed - o["elapsed"][-1])

                # Per-operation throughput
                tlist = map(lambda e:o["bsize"]/e, o["elapsed"][1:-1])
                opavg = num.average(tlist)
                opmin = num.min(tlist)
                opmax = num.max(tlist)
                opstd = num.std(tlist)

                self.create_table(o["name"], self.FORMATS["io"], overwrite)
                self.cur.execute(
                    "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    % o["name"], (res.hid, res.pid, res.tid, o["fsize"], 
                      o["bsize"], o["elapsed"], o['synctime'],
                      agg, aggnoclose, opavg, opmin, opmax, opstd))

    def select_rawdata_all(self, table):
        self.cur.execute("SELECT * FROM %s" % table)
        return self.cur.fetchall()
    
    def select_rawdata_cols(self, table, cols):
        self.cur.execute("SELECT %s FROM %s" % (cols, table))
        return self.cur.fetchall()

    def select_rawdata_hid(self, table, hid):
        self.cur.execute("SELECT * FROM %s WHERE hid=%d" 
            % (table, hid))
        return self.cur.fetchall()
        
    def get_hids(self, oper):
        self.cur.execute("SELECT hid FROM %s GROUP BY hid" % oper)
        return map(lambda (v,):v, self.cur.fetchall())

    def get_pids(self, oper):
        self.cur.execute("SELECT pid FROM %s GROUP BY pid" % oper)
        return map(lambda (v,):v, self.cur.fetchall())
    
    def get_tids(self, oper):
        self.cur.execute("SELECT tid FROM %s GROUP BY tid" % oper)
        return map(lambda (v,):v, self.cur.fetchall())

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
        return self.cur.fetchall()
    
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
            
            # time.time() may have low resolution and cause zero elapsed time
            # use average value as subsititions of zero values
            if 0 in dat:
                avg = num.average(dat)
                assert avg != 0
                _dat = []
                for d in dat:
                    if d == 0:
                        _dat.append(avg)
                    else:
                        _dat.append(d)
                dat = _dat

            if optype == OPTYPE_META:
                thlist = map(lambda t:1.0/t, dat)
                thagg = opcnt / num.sum(dat)
            elif optype == OPTYPE_IO:
                thlist = map(lambda t:bsize/t, dat)
                thagg = fsize / num.sum(dat)
            thmin = num.min(thlist)
            thmax = num.max(thlist)
            thavg = num.average(thlist)
            thstd = num.std(thlist)
            self._ins_aggdata(table, (hid,pid,tid,op,optype,thmin,thmax,thavg,
                thagg,thstd,sync))
    
    def _get_sync_time(self, listofsync):
        # TODO
        #return num.max(listofsync)
        return num.average(listofsync)

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
                thmin = num.min(thlist)
                thmax = num.max(thlist)
                thavg = num.average(thlist)
                thstd = num.std(thlist)
                self._ins_aggdata(table, \
                    (h,-1,n_threads,o,optype,thmin,thmax, \
                     thavg,thagg,thstd,tm))
    
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
            n_threads = int(num.sum(tidlist))
            tm = self._get_sync_time(tmlist)
            if optype == OPTYPE_META:
                thagg = n_threads * opcnt / tm
            elif optype == OPTYPE_IO:
                thagg = n_threads * fsize / tm
            thmin = num.min(thlist)
            thmax = num.max(thlist)
            thavg = num.average(thlist)
            thstd = num.std(thlist)
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
            thputlist.append(len(dat)/num.sum(map(lambda (s, e):e-s, dat)))
        thputavg = num.average(thputlist)
        thputmin = num.min(thputlist)
        thputmax = num.max(thputlist)
        thputstd = num.std(thputlist)
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
            thputlist.append(len(dat)/num.sum(map(lambda (s, e):e-s, dat)))
        thputavg = num.average(thputlist)
        thputmin = num.min(thputlist)
        thputmax = num.max(thputlist)
        thputstd = num.std(thputlist)
        return thputavg, thputmin, thputmax, thputstd, thputlist
    
    def io_get_data(self, **where):
        qstr = "SELECT data FROM io %s"
        wstr = " AND ".join(map(lambda k:"%s='%s'" % (k, where[k]), 
            where.keys()))
        if wstr != "": wstr = "WHERE %s" % wstr
        qstr = qstr % wstr
        self.cur.execute(qstr)
        return map(lambda (v,):self._str2obj(v), self.cur.fetchall())
