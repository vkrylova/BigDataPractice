import psycopg
from psycopg.rows import dict_row
from typing import cast, LiteralString

"""
db.py

This module provides a PostgreSQL client using psycopg3.

It is responsible for:
- establishing a connection to a PostgreSQL database
- executing SQL commands (INSERT, UPDATE, CREATE INDEX, etc.)
- fetching query results as dictionaries
- supporting COPY FROM STDIN for bulk data loading
- closing the database connection safely
"""


class DatabaseClient:
    def __init__(self, dsn: str = "postgresql://postgres:admin@localhost:5432/postgres") -> None:
        """
        Initializes the DatabaseClient with a DSN.
        Args:
            dsn (str): Data Source Name for PostgreSQL connection.
                       Defaults to "postgresql://postgres:admin@localhost:5432/postgres".
        """
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    def connect(self) -> None:
        """
        Establishes a connection to the PostgreSQL database.

        Raises:
            psycopg.OperationalError: If connection fails.
        """
        self._conn = psycopg.connect(dsn=self._dsn, row_factory=dict_row)

    def execute(self, sql_query: str, params: tuple | None = None) -> None:
        """
        Executes a SQL command that modifies data (INSERT, UPDATE, DELETE, CREATE, etc.).

        Args:
            sql_query (str): SQL statement to execute.
            params (tuple | None): Optional parameters for parameterized queries.

        Raises:
            RuntimeError: If called before connecting to the database.
        """
        if not self._conn:
            raise RuntimeError("Database not connected")
        with self._conn.cursor() as cur:
            cur.execute(cast(LiteralString, sql_query), params)
            self._conn.commit()

    def fetch(self, sql_query: str, params: tuple | None = None) -> list[dict]:
        """
        Executes a SELECT query and returns results as a list of dictionaries.

        Args:
            sql_query (str): SQL SELECT statement.
            params (tuple | None): Optional parameters for parameterized queries.

        Returns:
            list[dict]: List of rows as dictionaries, where column names are keys.

        Raises:
            RuntimeError: If called before connecting to the database.
        """
        if not self._conn:
            raise RuntimeError("Database not connected")
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(cast(LiteralString, sql_query), params)
            return cur.fetchall()

    def close(self) -> None:
        """
        Closes the database connection.
        """
        if self._conn:
            self._conn.close()
            self._conn = None
