import os
import logging
from pathlib import Path
import petl as etl


class Storage:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = None
        self.is_mysql = self._is_mysql()

    def _is_mysql(self):
        """Check if we should use MySQL based on environment or db_path"""
        return (os.getenv('DB_HOST') or
                self.db_path == 'mysql' or
                'mysql' in str(self.db_path).lower())

    def __enter__(self):
        self.connect()
        self._ensure_tables_exist()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Establish database connection"""
        try:
            if self.is_mysql:
                try:
                    import mysql.connector
                except ImportError:
                    logging.error("mysql-connector-python not installed. Install with: pip install mysql-connector-python")
                    raise ImportError("mysql-connector-python package is required for MySQL connections")
                    
                self.connection = mysql.connector.connect(
                    host=os.getenv('DB_HOST', 'mysql'),
                    user=os.getenv('DB_USER', 'pwconvert'),
                    password=os.getenv('DB_PASSWORD', 'pwconvert123'),
                    database=os.getenv('DB_NAME', 'pwconvert'),
                    autocommit=True,
                    connection_timeout=30
                )
                logging.info("Connected to MySQL database")
            else:
                import sqlite3
                # Create directory if it doesn't exist
                Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
                self.connection = sqlite3.connect(
                    self.db_path,
                    timeout=30,
                    check_same_thread=False
                )
                self.connection.row_factory = sqlite3.Row
                logging.info(f"Connected to SQLite database: {self.db_path}")

            return self.connection
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def _ensure_tables_exist(self):
        """Create tables if they don't exist"""
        try:
            cursor = self.connection.cursor()

            if self.is_mysql:
                # MySQL table creation
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS file (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        path VARCHAR(1000) NOT NULL,
                        size BIGINT,
                        mime VARCHAR(255),
                        version VARCHAR(100),
                        status ENUM('new', 'processing', 'converted', 'failed', 'accepted', 'skipped', 'protected', 'timeout', 'deleted', 'removed') DEFAULT 'new',
                        puid VARCHAR(50),
                        source_id INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        status_ts TIMESTAMP NULL,
                        error_message TEXT,
                        target_path VARCHAR(1000),
                        kept BOOLEAN DEFAULT TRUE,
                        original BOOLEAN DEFAULT TRUE,
                        finished BOOLEAN DEFAULT FALSE,
                        subpath VARCHAR(500),
                        INDEX idx_status (status),
                        INDEX idx_path (path(255)),
                        INDEX idx_source_id (source_id)
                    )
                """)
            else:
                # SQLite table creation
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS file (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        path TEXT NOT NULL,
                        size INTEGER,
                        mime TEXT,
                        version TEXT,
                        status TEXT DEFAULT 'new',
                        puid TEXT,
                        source_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status_ts TIMESTAMP,
                        error_message TEXT,
                        target_path TEXT,
                        kept BOOLEAN DEFAULT 1,
                        original BOOLEAN DEFAULT 1,
                        finished BOOLEAN DEFAULT 0,
                        subpath TEXT
                    )
                """)

            # Create indexes for SQLite
            if not self.is_mysql:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON file(status)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON file(path)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_id ON file(source_id)")

            cursor.close()
            logging.info("Database tables ensured")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")
            raise

    def append_rows(self, table):
        """Insert rows from petl table into database"""
        try:
            cursor = self.connection.cursor()
            rows = list(etl.dicts(table))

            if not rows:
                return 0

            # Get column names from first row
            columns = list(rows[0].keys())

            # Create INSERT statement
            if self.is_mysql:
                placeholders = ', '.join(['%s'] * len(columns))
                insert_sql = f"INSERT INTO file ({', '.join(columns)}) VALUES ({placeholders})"
            else:
                placeholders = ', '.join(['?'] * len(columns))
                insert_sql = f"INSERT INTO file ({', '.join(columns)}) VALUES ({placeholders})"

            # Insert rows
            for row in rows:
                values = [row.get(col) for col in columns]
                cursor.execute(insert_sql, values)

            cursor.close()
            logging.info(f"Inserted {len(rows)} rows into database")
            return len(rows)

        except Exception as e:
            logging.error(f"Error inserting rows: {e}")
            raise

    def get_row_count(self, conds=None, params=None):
        """Get count of rows matching conditions"""
        try:
            cursor = self.connection.cursor()

            if conds and params:
                sql = f"SELECT COUNT(*) as count FROM file WHERE {conds}"
                cursor.execute(sql, params)
            else:
                cursor.execute("SELECT COUNT(*) as count FROM file")

            result = cursor.fetchone()
            cursor.close()

            if self.is_mysql:
                return result[0]
            else:
                return result['count']

        except Exception as e:
            logging.error(f"Error getting row count: {e}")
            return 0

    def get_conds(self, mime=None, puid=None, status=None, subpath=None,
                  ext=None, from_path=None, to_path=None, timestamp=None,
                  reconvert=False, retry=False, finished=None, original=None):
        """Build WHERE conditions and parameters"""
        conditions = []
        params = []

        if mime:
            conditions.append("mime = %s" if self.is_mysql else "mime = ?")
            params.append(mime)

        if puid:
            conditions.append("puid = %s" if self.is_mysql else "puid = ?")
            params.append(puid)

        if status:
            conditions.append("status = %s" if self.is_mysql else "status = ?")
            params.append(status)

        if subpath:
            conditions.append("subpath = %s" if self.is_mysql else "subpath = ?")
            params.append(subpath)

        if from_path:
            conditions.append("path >= %s" if self.is_mysql else "path >= ?")
            params.append(from_path)

        if to_path:
            conditions.append("path < %s" if self.is_mysql else "path < ?")
            params.append(to_path)

        if finished is not None:
            conditions.append("finished = %s" if self.is_mysql else "finished = ?")
            params.append(finished)

        if original is not None:
            conditions.append("original_file = %s" if self.is_mysql else "original_file = ?")
            params.append(original)

        if reconvert:
            conditions.append("status IN ('converted', 'failed', 'timeout')")

        if retry:
            conditions.append("status = 'failed'")

        if not conditions:
            return "1=1", []

        return " AND ".join(conditions), params

    def get_rows(self, conds, params, limit=None, offset=None):
        """Get rows matching conditions"""
        try:
            cursor = self.connection.cursor()

            sql = f"SELECT * FROM file WHERE {conds}"

            if limit:
                sql += f" LIMIT {limit}"
            if offset:
                sql += f" OFFSET {offset}"

            cursor.execute(sql, params)

            if self.is_mysql:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                # Convert to list of dicts
                result = [dict(zip(columns, row)) for row in rows]
            else:
                result = [dict(row) for row in cursor.fetchall()]

            cursor.close()

            # Convert to petl table
            if result:
                return etl.fromdicts(result)
            else:
                return etl.fromdicts([])

        except Exception as e:
            logging.error(f"Error getting rows: {e}")
            return etl.fromdicts([])

    def update_row(self, row_data):
        """Update a single row"""
        try:
            cursor = self.connection.cursor()

            # Build UPDATE statement
            set_clauses = []
            params = []

            for key, value in row_data.items():
                if key != 'id':
                    set_clauses.append(f"{key} = %s" if self.is_mysql else f"{key} = ?")
                    params.append(value)

            params.append(row_data['id'])

            sql = f"UPDATE file SET {', '.join(set_clauses)} WHERE id = %s" if self.is_mysql else f"UPDATE file SET {', '.join(set_clauses)} WHERE id = ?"

            cursor.execute(sql, params)
            cursor.close()

        except Exception as e:
            logging.error(f"Error updating row: {e}")
            raise

    def update_status(self, conds, params, new_status):
        """Update status for multiple rows"""
        try:
            cursor = self.connection.cursor()

            sql = f"UPDATE file SET status = %s WHERE {conds}" if self.is_mysql else f"UPDATE file SET status = ? WHERE {conds}"
            cursor.execute(sql, [new_status] + params)
            cursor.close()

        except Exception as e:
            logging.error(f"Error updating status: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.connection:
            try:
                self.connection.close()
                logging.info("Database connection closed")
            except:
                pass
            finally:
                self.connection = None

