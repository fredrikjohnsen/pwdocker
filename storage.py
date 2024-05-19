import os
import sqlite3
import datetime
from sqlite3 import Connection
from typing import Optional

import petl
from petl import appenddb, fromdb, todb


class Storage:
    _create_table_str = """
    CREATE TABLE file(
        id integer primary key,
        path varchar(255) not null,
        size decimal,
        puid varchar(10),
        format varchar(100),
        version varchar(32),
        mime varchar(100),
        encoding varchar(30),
        ext varchar(10),
        status varchar(10),
        status_ts datetime default current_timestamp,
        kept boolean,
        source_id int
    );"""

    _create_view_file_root = """
    create view file_root as
    with cte as (
        select id, path, source_id, id as root_id
        from file
        where source_id is null
        union
        select f.id, f.path, f.source_id, h.root_id as root_id
        from file f
        join cte h
        on h.id = f.source_id
        where f.id != f.source_id
    )
    select * from cte
    order by path;
    """

    _update_str = """
        UPDATE file
        SET path = :path, size = :size, puid = :puid, format = :format,
            version = :version, mime = :mime, encoding = :encoding, ext = :ext,
            status = :status, status_ts = :status_ts, kept = :kept
        WHERE id = :id
        """

    _add_converted_file_str = """
    insert into file (path, status, source_id)
    values (:path, :status, :source_id)
    """

    _delete_str = """
    delete from file where id = :id
    """

    def __init__(self, path: str):
        self._conn = Optional[Connection]
        self.path = path

    def __enter__(self):
        self.load_data_source()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_data_source()

    def load_data_source(self):
        storage_dir = os.path.dirname(self.path)
        if not os.path.isdir(storage_dir):
            os.makedirs(storage_dir)

        self._conn = sqlite3.connect(self.path)
        query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='file'        
        """
        table = self._conn.execute(query)
        if len(table.fetchall()) <= 0:
            self._conn.execute(self._create_table_str)
            self._conn.execute(self._create_view_file_root)
            self._conn.commit()

    def close_data_source(self):
        if self._conn:
            self._conn.close()

    def import_rows(self, table):
        # import rows
        todb(table, self._conn, "file")

    def append_rows(self, table):
        # select the first row (primary key) and filter away rows that already exist
        self._conn.row_factory = lambda cursor, row: row[0]
        file_names = self._conn.execute("SELECT path FROM file").fetchall()
        table = petl.select(
            table,
            lambda rec: (rec.path not in file_names)
        )
        # append new rows
        appenddb(table, self._conn, "file")
        self._conn.row_factory = None

    def update_row(self, data: dict):
        self._conn.execute(self._update_str, data)
        self._conn.commit()

    def add_row(self, data: dict):
        self._conn.execute(self._add_converted_file_str, data)
        # This gets commmited when `update_row` is called
        # Got 'unable to open database file' when calling this
        # and `update_row` rigth after in convert.py

    def delete_row(self, data: dict):
        self._conn.execute(self._delete_str, data)
        self._conn.commit()

    def get_subfolders(self, conds, params):
        cursor = self._conn.cursor()
        query = """
        SELECT DISTINCT substr(path, 0, instr(path, '/')) as dir
        FROM   file
        """

        if len(conds):
            query += "\nwhere " + ' AND '.join(conds)

        cursor.execute(query, params)

        rows = cursor.fetchall()
        folders = []
        for row in rows:
            folders.append(row[0])

        return folders

    def get_row_count(self, conds, params):
        cursor = self._conn.cursor()
        query = "SELECT COUNT(*) FROM file"

        if len(conds):
            query += " where " + ' AND '.join(conds)

        cursor.execute(query, params)

        return cursor.fetchone()[0]

    def get_all_rows(self, unpacked_path):

        if unpacked_path:
            unpacked_path = os.path.join(unpacked_path, '')
            unpacked_path = unpacked_path.replace("'", "''")

        sql = f"""
        SELECT * FROM file
        where path like '{str(unpacked_path)}%'
          and source_id IS NULL
        """
        params = []

        return fromdb(
            self._conn,
            sql,
            params
        )

    def get_new_rows(self):

        sql = """
        SELECT * FROM file
        WHERE  status IS NULL
        """

        return fromdb(
            self._conn,
            sql,
        )

    def get_conditions(self, mime=None, puid=None, status=None, reconvert=False,
                       finished=False, subpath=None, from_path=None, to_path=None,
                       timestamp=None, original=False):

        conds = []
        params = []

        if original:
            conds.append("source_id IS NULL")

        if not finished and not reconvert:
            conds.append('(status is null or status not in (?, ?, ?))')
            params.append('converted')
            params.append('accepted')
            params.append('removed')
        elif finished:
            conds.append('status in (?, ?, ?)')
            params.append('converted')
            params.append('accepted')
            params.append('removed')

        if mime:
            conds.append("mime = ?")
            params.append(mime)

        if puid:
            conds.append("puid = ?")
            params.append(puid)

        if status:
            conds.append("status = ?")
            params.append(status)

        if subpath:
            conds.append("path like ?")
            params.append(subpath + '%')

        if from_path:
            conds.append("path >= ?")
            params.append(from_path)

        if to_path:
            conds.append("path < ?")
            params.append(to_path)

        if timestamp:
            conds.append("status_ts < ?")
            params.append(timestamp)

        return conds, params

    def get_rows(self, conds, params):

        select = "SELECT * from file"

        if len(conds):
            select += " WHERE " + ' AND '.join(conds)

        return fromdb(self._conn, select, params)

    def get_failed_rows(self, mime: str = None):
        select = """
            SELECT path FROM file
            WHERE  status IS NOT NULL AND status IN(?, ?, ?)
        """
        params = ['timeout', 'failed', 'protected']

        if mime:
            select += " AND mime = ?"
            params.append(mime)

        return fromdb(self._conn, select, params)

    def get_skipped_rows(self, mime: str = None):
        select = """
            SELECT path FROM file
            WHERE  status IS NOT NULL AND status IN(?)
        """
        params = ['skipped']

        if mime:
            select += " AND mime = ?"
            params.append(mime)

        return fromdb(self._conn, select, params)

    def get_new_mime_types(self):
        return fromdb(
            self._conn,
            """
            SELECT count(*) as no, mime FROM file
            WHERE status is NULL
            GROUP BY mime
            ORDER BY count(*) desc
            """
        )

    def get_unconv_mime_types(self):
        return fromdb(
            self._conn,
            """
            SELECT count(*) as no, mime FROM file
            WHERE status is NULL OR status NOT IN(?)
            GROUP BY mime
            ORDER BY count(*) desc
            """,
            ['converted']
        )

    def delete_descendants(self, id):
        sql = """
        with recur as (
        select a.id, a.id as orig, a.source_id from file a
        where a.id = ?
        union all
        select b.id, c.orig as orig, b.source_id from file b
        inner join recur c on c.id = b.source_id
        )
        delete from file
        where id in (select id from recur where source_id is not null)
        """

        params = [id]
        self._conn.execute(sql, params)
        self._conn.commit()
