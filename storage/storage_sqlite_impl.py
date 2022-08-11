import os
import sqlite3
from sqlite3 import Connection
from typing import Optional, Any, List

import petl
from petl import appenddb, fromdb

from storage import ConvertStorage
from util import Result


class StorageSqliteImpl(ConvertStorage):
    _create_table_str = '''
    CREATE TABLE File(
        source_file_path TEXT PRIMARY KEY NOT NULL,
        file_size DECIMAL,
        modified  TEXT,
        errors TEXT,
        format TEXT,
        id TEXT,
        version TEXT,
        mime_type TEXT,
        norm_file_path TEXT,
        result TEXT,
        source_directory TEXT
    );'''

    _update_result_str = '''
        UPDATE File 
        SET file_size = ?, modified = ?,  errors = ?, id = ?, 
            format = ?, version = ?, mime_type = ?, norm_file_path = ?, result = ?, source_directory = ? 
        WHERE source_file_path = ?
        '''

    def __init__(self, storage_dir: str, storage_name: str, preserve_existing_data: bool = True):
        self._conn = Optional[Connection]
        self.storage_dir = storage_dir
        self.storage_name = storage_name
        self.preserve_existing_data = preserve_existing_data

    def __enter__(self):
        self.load_data_source()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_data_source()

    def load_data_source(self):
        if not os.path.isdir(self.storage_dir):
            os.makedirs(self.storage_dir)

        storage_path = f'{self.storage_dir}/{self.storage_name}'
        self._conn = sqlite3.connect(storage_path)
        print(f'Opened DB {self.storage_name} successfully')
        if not self.preserve_existing_data:
            self._conn.execute('DROP TABLE IF EXISTS File')
            self._conn.commit()

        table = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='File'")
        if len(table.fetchall()) <= 0:
            self._conn.execute(self._create_table_str)
            self._conn.commit()

    def close_data_source(self):
        if self._conn:
            self._conn.close()

    def append_rows(self, table):
        # select the first row (primary key) and filter away rows that already exist
        self._conn.row_factory = lambda cursor, row: row[0]
        file_names = self._conn.execute('SELECT source_file_path FROM File').fetchall()
        table = petl.select(table, lambda rec: rec.source_file_path not in file_names)
        # append new rows
        appenddb(table, self._conn, 'File')
        self._conn.row_factory = None

    def update_row(self, src_path: str, data: List[Any]):
        data.append(src_path)
        data.pop(0)
        self._conn.execute(self._update_result_str, data)
        self._conn.commit()

    def get_unconverted_rows(self, source_dir: str):
        return fromdb(
            self._conn,
            '''
            SELECT * FROM File 
            WHERE source_directory = ? AND result IS NULL OR result NOT IN(?, ?)
            ''',
            [source_dir, Result.SUCCESSFUL, Result.MANUAL]
        )

    def get_converted_rows(self, source_dir: str):
        return fromdb(
            self._conn,
            ''' 
            SELECT source_file_path FROM File
            WHERE source_directory = ? AND result IS NOT NULL AND result IN(?, ?)
            ''',
            [source_dir, Result.SUCCESSFUL, Result.MANUAL]
        )
