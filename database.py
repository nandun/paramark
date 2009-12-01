#############################################################################
# ParaMark:  A Parallel/Distributed File Systems Benchmark
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

# Data persistent by sqlite:
# * Benchmark data
# * Filesysmtem sampling data

import sqlite3

DB_FORMAT_VERSION = 1

class Database:
    """
    Database base class
    """
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.db = sqlite3.connect(dbfile)
        self.cur = self.db.cursor()

        self.tables = []
    
    def close_database(self):
        self.db.commit()
        self.db.close()
        self.db = None

    def create_tables(self):
        # table env always exists
        self.cur.execute("DROP TABLE IF EXISTS env")
        self.cur.execute("CREATE TABLE IF NOT EXISTS env "
            "(item TEXT, value TEXT)")
        self.tables.append("env")

    def drop_tables(self):
        for table in self.tables:
            self.cur.execute("DROP TABLE IF EXISTS %s" % table)

    def commit_data(self):
        self.db.commit()

    # table primitivies
    def env_insert(self, item, value):
        self.cur.execute("INSERT INTO env VALUES (?,?)", (item, value))
    
    def env_select(self, fields="*"):
        self.cur.execute("SELECT %s FROM env" % fields)
        return self.cur.fetchall()

class SerialBenchmarkDB(Database):
    """
    Benchmark database, contains following tables: 
        runtime: runtime environment values
    """
    def __init__(self, dbfile):
        Database.__init__(self, dbfile)

    def create_tables(self):
        Database.create_tables(self)

        # table: io
        self.cur.execute("DROP TABLE IF EXISTS io")
        self.cur.execute("CREATE TABLE IF NOT EXISTS io "
            "(oper TEXT, nproc INTEGER, fsize INTEGER, bsize INTEGER, "
            "exectime DOUBLE, mintime DOUBLE, maxtime DOUBLE, "
            "throughput DOUBLE)")
        self.tables.append("io")
        # set database format version
        self.cur.execute("INSERT INTO env VALUES (?,?)", ("dbver", 
            DB_FORMAT_VERSION))

        # table: meta
        self.cur.execute("DROP TABLE IF EXISTS meta")
        self.cur.execute("CREATE TABLE IF NOT EXISTS meta "
            "(oper TEXT, nproc INTEGER, count INTEGER, factor INTEGER, "
            "exectime DOUBLE, mintime DOUBLE, maxtime DOUBLE, "
            " throughput DOUBLE)")
        self.tables.append("meta")

    # table: io
    def io_insert(self, row):
        self.cur.execute("INSERT INTO io VALUES (?,?,?,?,?,?,?,?)", row)
    
    def io_select(self, fields="*"):
        self.cur.execute("SELECT %s FROM io" % fields)
        return self.cur.fetchall()
    
    # table: meta
    def meta_insert(self, row):
        self.cur.execute("INSERT INTO meta VALUES (?,?,?,?,?,?,?,?)", row)
    
    def meta_select(self, fields="*"):
        self.cur.execute("SELECT %s FROM meta" % fields)
        return self.cur.fetchall()
