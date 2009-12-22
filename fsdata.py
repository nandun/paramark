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
# File System Benchmark Data Persistence
#

import sqlite3
import cPickle

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
        
        # rawdata
        self.cur.execute("DROP TABLE IF EXISTS rawdata")
        self.cur.execute("CREATE TABLE IF NOT EXISTS rawdata"
            "(item TEXT, value BLOB)")
        self.tables.append("rawdata")

    def drop_tables(self):
        for tables in self.tables:
            self.cur.execute("DROP TABLE IF EXISTS %s" % table)

    def commit(self):
        self.db.commit()
    
    # Pickle
    def obj2str(self, obj):
        return cPickle.dumps(obj)
    
    def str2obj(self, objs):
        return cPickle.loads(objs)

    # Table Operations
    def runtime_ins(self, item, value):
        self.cur.execute("INSERT INTO runtime VALUES (?,?)", (item, value))
    
    def runtime_sel(self, fields="*"):
        self.cur.execute("SELECT %s from runtime" % fields)
        return self.cur.fetchall()
    
    def rawdata_ins(self, item, value):
        self.cur.execute("INSERT INTO rawdata VALUES (?,?)", (item, value))
