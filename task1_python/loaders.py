import json
from io import StringIO

from db import DatabaseClient

"""
loaders.py

This module contains data loader classes responsible for importing JSON files
into PostgreSQL tables using COPY FROM STDIN.

The loaders handle:
- reading source JSON files
- transforming JSON records into a CSV-like stream in memory
- performing high-performance bulk inserts into PostgreSQL via COPY

Supported data sources:
- rooms.json: list of rooms with id and name
- students.json: list of students with personal details and assigned room
"""


class RoomsLoader:
    """
    Loader for importing room data from a JSON file into the rooms table.

    Expected JSON structure:
        [
            {"id": 1, "name": "Room A"},
            {"id": 2, "name": "Room B"}
        ]

    The loader converts JSON data into an in-memory CSV stream and uses
    COPY FROM STDIN for efficient bulk insertion.
    """

    def __init__(self, db: DatabaseClient) -> None:
        """
        Initializes the RoomsLoader.

        Args:
            db (DatabaseClient): Active database client instance.
        """
        self.db = db

    def load(self, path: str) -> None:
        """
        Loads room data from a JSON file into the rooms table.

        Args:
            path (str): Path to the rooms.json file.
        """
        with open(path, 'r', encoding='utf-8') as f:
            rooms = json.load(f)

        # create in-memory CSV
        csv_buffer = StringIO()
        for room in rooms:
            csv_buffer.write(f"{room['id']}, {room['name']}\n")
        csv_buffer.seek(0)
        # COPY FROM STDIN
        sql = f"COPY rooms(id, name) FROM STDIN WITH CSV"
        with self.db.conn.cursor() as cursor:
            cursor.copy(sql, csv_buffer)


class StudentsLoader:
    """
    Loader for importing student data from a JSON file into the students table.

    Expected JSON structure:
        [
            {
                "id": 1,
                "name": "Alice",
                "birthday": "2000-01-01",
                "sex": "F",
                "room_id": 101
            }
        ]
    """

    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    def load(self, path: str) -> None:
        """
        Load students.json into the students table using COPY FROM STDIN
        """
        with open(path, 'r', encoding='utf-8') as file:
            student = json.load(file)

        # crate in-memory CSV
        csv_buffer = StringIO()
        for student in student:
            # ensure proper CSV formatting
            csv_buffer.write(
                f"{student['id']},"
                f"{student['name']},"
                f"{student['birthday']},"
                f"{student['sex']},"
                f"{student['room_id']}\n"
            )
        csv_buffer.seek(0)
        # COPY FROM STDIN
        sql = f"COPY students(id, name, birthday, sex, room_id) FROM STDIN WITH CSV"
        with self._db.conn.cursor() as cursor:
            cursor.copy(sql, csv_buffer)
