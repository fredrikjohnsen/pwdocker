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
    CREATE TABLE File(
        source_path TEXT NOT NULL,
        file_size DECIMAL,
        puid TEXT,
        format TEXT,
        version TEXT,
        mime_type TEXT,
        norm_path TEXT,
        result TEXT,
        moved_to_target INTEGER DEFAULT 0,
        PRIMARY KEY (source_path)
    );"""

    _update_result_str = """
        UPDATE File 
        SET file_size = ?, puid = ?, format = ?, version = ?, mime_type = ?,
        norm_path = ?, result = ?, moved_to_target = ?
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
        table = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='File'")
        if len(table.fetchall()) <= 0:
            self._conn.execute(self._create_table_str)
            self._conn.commit()

    def close_data_source(self):
        if self._conn:
            self._conn.close()

    def import_rows(self, table):
        # import rows
        todb(table, self._conn, "File")

    def append_rows(self, table):
        # select the first row (primary key) and filter away rows that already exist
        self._conn.row_factory = lambda cursor, row: row[0]
        file_names = self._conn.execute("SELECT source_path FROM File").fetchall()
        table = petl.select(
            table,
            lambda rec: (rec.source_path not in file_names)
        )
        # append new rows
        appenddb(table, self._conn, "File")
        self._conn.row_factory = None

    def update_row(self, src_path: str, data: List[Any]):
        data.append(src_path)
        data.pop(0)
        self._conn.execute(self._update_result_str, data)
        self._conn.commit()

    def get_row_count(self, mime_type):
        cursor = self._conn.cursor()
        query = "SELECT COUNT(*) FROM File"
        params = []

        if mime_type:
            query += " WHERE mime_type = ?"
            params.append(mime_type)

        cursor.execute(query, params)

        return cursor.fetchone()[0]

    def get_all_rows(self):
        return fromdb(
            self._conn,
            """
            SELECT * FROM File 
            """,
        )

    def get_new_rows(self):
        return fromdb(
            self._conn,
            """
            SELECT * FROM File
            WHERE  result IS NULL
            """,
            [source_dir]
        )

    def get_unconverted_rows(self, mime_type: str):
        select = """
            SELECT * FROM File
            WHERE  result IS NULL OR result NOT IN(?)
        """
        params = [Result.SUCCESSFUL]

        if mime_type:
            select += " AND mime_type = ?"
            params.append(mime_type)

        return fromdb(self._conn, select, params)

    def get_converted_rows(self, mime_type: str):
        select = """
            SELECT source_path FROM File
            WHERE  result IS NOT NULL AND result IN(?)
        """
        params = [Result.SUCCESSFUL]

        if mime_type:
            select += " AND mime_type = ?"
            params.append(mime_type)

        return fromdb(self._conn, select, params)

    def get_new_mime_types(self):
        return fromdb(
            self._conn,
            """
            SELECT count(*) as no, mime_type FROM File
            WHERE result is NULL
            GROUP BY mime_type
            ORDER BY count(*) desc
            """
        )

    def get_unconv_mime_types(self):
        return fromdb(
            self._conn,
            """
            SELECT count(*) as no, mime_type FROM File
            WHERE result is NULL OR result NOT IN(?)
            GROUP BY mime_type
            ORDER BY count(*) desc
            """,
            [Result.SUCCESSFUL]
        )
