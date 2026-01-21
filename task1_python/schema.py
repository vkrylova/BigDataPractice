from db import DatabaseClient

"""
    schema.py
    
    Manages the database schema, including creation of required tables.
"""


class SchemaManager:

    def __init__(self, db: DatabaseClient) -> None:
        self.db = db

    def create_tables(self) -> None:
        """
        Creates the required database tables: `rooms` and `students`.

        Drops the tables first if they already exist. Defines primary keys,
        foreign key relationship between students and rooms, and column types.

        Raises:
            Any database exception propagated from db.execute if the SQL fails.
        """

        self.db.execute("""Drop TABLE IF EXISTS students""")
        self.db.execute("""Drop TABLE IF EXISTS rooms""")
        self.db.execute("""CREATE TABLE rooms
                           (
                               id   INTEGER PRIMARY KEY NOT NULL,
                               name VARCHAR(255)        NOT NULL
                           );
                        """)

        self.db.execute("""CREATE TABLE students
                           (
                               id       INTEGER PRIMARY KEY           NOT NULL,
                               name     VARCHAR(255)                  NOT NULL,
                               birthday DATE                          NOT NULL,
                               sex      VARCHAR(5)                    NOT NULL,
                               room     INTEGER REFERENCES rooms (id) NOT NULL

                           );
                        """)
