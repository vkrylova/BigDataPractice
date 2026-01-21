import json
from db import DatabaseClient


class RoomsLoader:
    """
    Loader for importing room data from a JSON file into the rooms table.

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
        Loads room data from a JSON file into the rooms table using COPY FROM STDIN.
        """

        with open(path, 'r', encoding='utf-8') as f:
            rooms = json.load(f)

        sql = "COPY rooms(id, name) FROM STDIN WITH (FORMAT CSV)"

        with self.db.conn.cursor() as cursor:
            with cursor.copy(sql) as copy:
                for room in rooms:
                    copy.write(f"{room['id']},{room['name']}\n")


class StudentsLoader:
    """
    Loader for importing student data from a JSON file into the students table.
    """

    def __init__(self, db: DatabaseClient) -> None:
        """
        Initializes the StudentsLoader.

        Args:
            db (DatabaseClient): Active database client instance.
        """
        self.db = db

    def load(self, path: str) -> None:
        """
        Load students.json into the students table using COPY FROM STDIN
        """

        with open(path, 'r', encoding='utf-8') as f:
            students = json.load(f)

        sql = "COPY students(id, name, birthday, sex, room) FROM STDIN WITH CSV"

        with self.db.conn.cursor() as cursor:
            with cursor.copy(sql) as copy:
                for student in students:
                    copy.write(
                        f"{student['id']},"
                        f"{student['name']},"
                        f"{student['birthday']},"
                        f"{student['sex']},"
                        f"{student['room']}\n"
                    )
