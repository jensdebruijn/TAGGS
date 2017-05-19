import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter, AsIs

from methods import function

from config import (
    POSTGRESQL_USER,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_HOST,
    POSTGRESQL_PORT
)


class PostgreSQL:
    def __init__(self, db):
        self.conn, self.cur = self._connect(POSTGRESQL_HOST, POSTGRESQL_PORT, POSTGRESQL_USER, POSTGRESQL_PASSWORD, db)

    def commit_chunk(self, query, mogr, items, size=1000):
        for chunk in function.chunker(items, size):
            self.do_query(query, mogr, chunk, commit=False)
        self.conn.commit()

    def do_query(self, query, mogr, items, commit=True):
        arg_str = ','.join(self.cur.mogrify(mogr, value).decode() for value in items)
        query_str = query.format(arg_str)
        self.cur.execute(query_str)
        if commit:
            self.conn.commit()

    def _connect(self, host, port, user, password, db):
        try:
            if password:
                connect_str = f"user='{user}' host='{host}' port='{port}' password='{password}'"
            else:
                connect_str = f"user='{user}' host='{host}' port='{port}'"
            try:
                conn = psycopg2.connect("dbname='{}' ".format(db) + connect_str)
            except psycopg2.OperationalError:
                conn = psycopg2.connect(connect_str)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("CREATE DATABASE {}".format(db))
                conn.close()
                conn = psycopg2.connect("dbname='{}' ".format(db) + connect_str)
            cur = conn.cursor()
            register_adapter(float, self._nan_to_null)
            self._register_numpy_types()
            self.conn = conn
            self.cur = cur
            return conn, cur

        except Exception as e:
            raise

    def _register_numpy_types(self):
        """Register the AsIs adapter for following types from numpy:
          - numpy.int8
          - numpy.int16
          - numpy.int32
          - numpy.int64
          - numpy.float16
          - numpy.float32
          - numpy.float64
        """
        for typ in ['int8', 'int16', 'int32', 'int64',
                    'float16', 'float32', 'float64']:
            register_adapter(np.__getattribute__(typ), AsIs)

    def _nan_to_null(self, f,
                     _NULL=psycopg2.extensions.AsIs('NULL'),
                     _Float=psycopg2.extensions.Float):
        if not np.isnan(f):
            return _Float(f)
        return _NULL

    def initialize_postgis(self):
        self.cur.execute('CREATE EXTENSION IF NOT EXISTS postgis')
        self.conn.commit()

    def create_aggregates(self):
        self.cur.execute("DROP AGGREGATE IF EXISTS array_accum (anyelement)")
        self.cur.execute("CREATE AGGREGATE array_accum (anyelement) \
            ( \
                sfunc = array_append, \
                stype = anyarray, \
                initcond = '{}' \
            ); \
        ")
        self.conn.commit()

    def get_columns(self, table):
        self.cur.execute("SELECT column_name \
            FROM information_schema.columns \
            WHERE table_schema = 'public' \
              AND table_name   = '{table}'".format(table=table))
        return [c for c, in self.cur.fetchall()]


if __name__ == '__main__':
    conn, cur = connect()
    cur.execute("SELECT * FROM timezones")
    print(cur.fetchone())
    cur.execute("SELECT * FROM geonames WHERE geonameid = '3579932'")
    print(cur.fetchone())
