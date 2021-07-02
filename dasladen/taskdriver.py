"""
Driver Module

Oracle Driver External Dependencies:
- cx_oracle: (c) 2016, 2017, Oracle and/or its affiliates.
             Portions Copyright 2007-2015, Anthony Tuininga.
             Portions Copyright 2001-2007, Computronix (Canada) Ltd., Edmonton, Alberta, Canada.
             BSD License (https://github.com/oracle/python-cx_Oracle/blob/master/LICENSE.txt)

MySQL Driver External Dependencies:
- PyMySQL: (c) 2010, 2013 PyMySQL contributors - MIT License (https://pypi.python.org/pypi/PyMySQL/0.7.1)

MSSQL Driver (via ODBC) External Dependencies:
- pyodbc: (c) Michael Kleehammer - MIT License (https://github.com/mkleehammer/pyodbc)

Postgres Driver External Dependencies:
- psycopg2: (c) Federico Di Gregorio, Daniele Varrazzo, Jason Erickson - LGPL License (https://github.com/psycopg/psycopg2)

Features:
- Connection to MS SQL
- Connection to MySQL
- Connection to Oracle
- Connection to PostgreSQL

"""

import os

from . import compat
try:
    import cx_Oracle as oracle
except ImportError:
    pass
try:
    import pyodbc as odbc
except ImportError:
    pass
try:
    import pymysql as mysql
except ImportError:
    pass
try:
    import psycopg2 as postgres
    import psycopg2.extras as postres_extras
except ImportError:
    pass


def get_env(value):
    """Return the [VAR] environment variable if starts with $env.[VAR]"""
    if len(value) > 5 and value.startswith("$env."):
        return os.environ[value[5:]]
    else:
        return value


class CursorProxy(object):
    """Proxy for cursor that not has support a executemany with iterators"""

    def __init__(self, cursor):
        self._cursor = cursor

    def executemany(self, statement, parameters, **kwargs):
        # convert parameters to a list
        parameters = list(parameters)
        # pass through to proxy cursor
        return self._cursor.executemany(statement, parameters, **kwargs)

    def __getattr__(self, item):
        return getattr(self._cursor, item)


class OracleDriver(object):
    """Driver for Oracle connections"""

    def __init__(self, config):
        self.config = config

    def get_db(self):
        conn = self.config
        host_address = conn.get("host", "localhost")
        port = conn.get("port", "1521")

        str_conn = "{}/{}@{}:{}/{}".format(get_env(conn["user"]), get_env(conn["pass"]),
                                           host_address, port, conn["service"])
        db = oracle.connect(str_conn)
        db.outputtypehandler = self.output_type_handler

        if "initializing" in conn:
            for sql in conn["initializing"]:
                db.cursor().execute(sql)

        return db

    def output_type_handler(self, cursor, name, defaultType, size, precision, scale):
        if defaultType in (STRING, FIXED_CHAR):
            if compat.PY2:
                return cursor.var(unicode, size, cursor.arraysize)
            else:
                return cursor.var(cx_Oracle.STRING, size, cursor.arraysize)

    def cursor(self, db):
        return CursorProxy(db.cursor())


class MSSQLDriver(object):
    """Driver for MS SQL connections via ODBC"""

    def __init__(self, config):
        self.config = config

    def get_db(self):
        conn = self.config
        db_charset = "CHARSET={};".format(conn["charset"]) if "charset" in conn else ""
        host_address = conn.get("host", "(local)")
        port = conn.get("port", "1433")

        if not conn["user"]:
            str_conn = ("DRIVER={{SQL Server}};SERVER={};"
                        "PORT={};DATABASE={};Trusted_Connection=yes;{}").format(
                host_address, port,
                conn["database"],
                db_charset)
        else:
            str_conn = ("DRIVER={{SQL Server}};SERVER={};"
                        "PORT={};DATABASE={};UID={};PWD={};{}").format(
                host_address, port,
                conn["database"],
                get_env(conn["user"]),
                get_env(conn["pass"]),
                db_charset)

        db = odbc.connect(str_conn)

        if "initializing" in conn:
            for sql in conn["initializing"]:
                db.cursor().execute(sql)
        return db

    def cursor(self, db):
        return db.cursor()


class MySQLDriver(object):
    """Driver for MySQL connections"""

    def __init__(self, config):
        self.config = config

    def get_db(self):
        conn = self.config
        db_charset = conn.get("charset", "utf8")
        host_address = conn.get("host", "localhost")
        port = conn.get("port", 3306)

        db = mysql.connect(host=host_address,
                           port=port,
                           user=get_env(conn["user"]),
                           password=get_env(conn["pass"]),
                           database=conn["database"],
                           charset=db_charset,
                           local_infile=1)
        # needed for petl work correctly
        db.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

        if "initializing" in conn:
            for sql in conn["initializing"]:
                db.cursor().execute(sql)
        return db

    def cursor(self, db):
        return db.cursor()


class PostgreBatchCursor():
    """Proxy that bypass executemany and run execute_batch on psycopg2 """

    def __init__(self, cursor):
        self._cursor = cursor

    def executemany(self, statement, parameters, **kwargs):
        return postres_extras.execute_batch(self._cursor, statement, parameters, **kwargs)

    def __getattr__(self, item):
        return getattr(self._cursor, item)    


class PostgreSQLDriver(object):
    """Driver for PostgreSQL connections"""

    def __init__(self, config):
        self.config = config

    def get_db(self):
        conn = self.config
        db_charset = conn.get("charset", "utf8")
        host_address = conn.get("host", "localhost")
        port = conn.get("port", 5432)
        db = postgres.connect(host=host_address,
							  port=port,
                              user=get_env(conn["user"]),
                              password=get_env(conn["pass"]),
                              database=conn["database"],
						      client_encoding=db_charset)

        if "initializing" in conn:
            for sql in conn["initializing"]:
                db.cursor().execute(sql)
        return db

    def cursor(self, db):
        return PostgreBatchCursor(db.cursor())
