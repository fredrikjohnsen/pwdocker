from typing import Any, List

from petl.io.db import DbView


class ConvertStorage:
    def load_data_source(self):
        """Load the datasource"""
        pass

    def close_data_source(self):
        """Closes the datasource connection"""
        pass

    def update_row(self, src_path: str, data: List[Any]):
        """Update a row in the store"""
        pass

    def get_converted_rows(self) -> DbView:
        """Returns source_path for the files that have been converted"""
        pass

    def get_unconverted_rows(self) -> DbView:
        """Returns rows for files that have not been converted yet"""
        pass

    def append_rows(self, table):
        """Appends rows to the table if they do not already exist"""
        pass
