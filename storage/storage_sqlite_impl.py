import os
import sqlite3
from sqlite3 import Connection
from typing import Optional, Any, List

import petl
from petl import appenddb, fromdb, todb

from storage import ConvertStorage
from util import Result


class StorageSqliteImpl(ConvertStorage):
    _create_table_str = """
    CREATE TABLE file(
        source_path TEXT NOT NULL,
        source_file_size DECIMAL,
        puid TEXT,
        format TEXT,
        version TEXT,
        source_mime_type TEXT,
        dest_path TEXT,
        result TEXT,
        dest_mime_type TEXT,
        moved_to_target INTEGER DEFAULT 0,
        PRIMARY KEY (source_path)
    );"""

    _update_result_str = """
        UPDATE file 
        SET source_file_size = ?, puid = ?, format = ?, version = ?, source_mime_type = ?,
        dest_path = ?, result = ?, dest_mime_type = ?, moved_to_target = ?
        WHERE source_path = ?
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
        file_names = self._conn.execute("SELECT source_path FROM file").fetchall()
        table = petl.select(
            table,
            lambda rec: (rec.source_path not in file_names)
        )
        # append new rows
        appenddb(table, self._conn, "file")
        self._conn.row_factory = None

    def update_row(self, source_path: str, data: List[Any]):
        data.append(source_path)
        data.pop(0)
        self._conn.execute(self._update_result_str, data)
        self._conn.commit()

    def get_row_count(self, mime_type=None, result=None):
        cursor = self._conn.cursor()
        query = "SELECT COUNT(*) FROM file"
        conds = []
        params = []

        if mime_type:
            conds.append("source_mime_type = ?")
            params.append(mime_type)

        if result:
            conds.append("result = ?")
            params.append(result)

        if len(conds):
            query += "\nWHERE " + ' AND '.join(conds)

        cursor.execute(query, params)

        return cursor.fetchone()[0]

    def get_all_rows(self, unpacked_path):

        if unpacked_path:
            unpacked_path = os.path.join(unpacked_path, '')

        return fromdb(
            self._conn,
            f"""
            SELECT * FROM file
            where source_path like '{str(unpacked_path)}%'
            """,
        )

    def get_new_rows(self):
        return fromdb(
            self._conn,
            """
            SELECT * FROM file
            WHERE  result IS NULL
            """,
        )

    def get_unconverted_rows(self, mime_type: str = None, result: str = None):
        select = """
            SELECT * FROM file
            WHERE  result IS NULL OR result NOT IN(?)
        """
        params = [Result.SUCCESSFUL]

        if mime_type:
            select += " AND source_mime_type = ?"
            params.append(mime_type)

        if result:
            select += " AND result = ?"
            params.append(result)

        return fromdb(self._conn, select, params)

    def get_converted_rows(self, mime_type: str = None):
        select = """
            SELECT source_path FROM file
            WHERE  result IS NOT NULL AND result IN(?)
        """
        params = [Result.SUCCESSFUL]

        if mime_type:
            select += " AND source_mime_type = ?"
            params.append(mime_type)

        return fromdb(self._conn, select, params)

    def get_new_mime_types(self):
        return fromdb(
            self._conn,
            """
            SELECT count(*) as no, source_mime_type FROM file
            WHERE result is NULL
            GROUP BY source_mime_type
            ORDER BY count(*) desc
            """
        )

    def get_unconv_mime_types(self):
        return fromdb(
            self._conn,
            """
            SELECT count(*) as no, source_mime_type FROM file
            WHERE result is NULL OR result NOT IN(?)
            GROUP BY source_mime_type
            ORDER BY count(*) desc
            """,
            [Result.SUCCESSFUL]
        )
