import psycopg
from psycopg.rows import dict_row
from typing import cast, LiteralString
import os


class DatabaseClient:
    """
    This module provides a PostgreSQL client using psycopg3.

    It is responsible for:
        - establishing a connection to a PostgreSQL database
        - fetching query results as dictionaries
        - closing the database connection safely
    """

    def __init__(self) -> None:
        """
        Creates a DatabaseClient instance without establishing
        a database connection.
        """

        self._conn: psycopg.Connection | None = None

    def connect(self) -> None:
        """
        Establishes a connection to the PostgreSQL database.

        Connection parameters are read from environment variables.

        Raises:
            psycopg.OperationalError: If connection fails.
        """

        self._conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME", "postgres"),
        )

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

    @property
    def conn(self) -> psycopg.Connection | None:
        return self._conn
