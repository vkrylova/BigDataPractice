"""
    queries.py

    Service class to execute database queries and manage indexes.
"""


class QueryService:

    def __init__(self, db) -> None:
        """
        Initializes the QueryService with a database client.

        Args:
            db (DatabaseClient): Active database client instance.
        """

        self.db = db

    def create_indexes(self) -> None:
        """
        Create database indexes to optimize queries.
        """

        self.db.execute("CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room);")
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);")
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_students_room_sex ON students(room, sex);")

    def _rooms_student_count(self) -> list[dict]:
        """
        Retrieve a list of all rooms with the number of students in each room.

        Returns:
            list[dict]: List of dictionaries, each representing a room and student count.
        """

        sql = """SELECT r.name, COUNT(s.id) AS student_count
                 FROM rooms r
                          LEFT JOIN students s ON r.id = s.room
                 GROUP BY r.id, r.name;
              """
        return self.db.fetch(sql)

    def _smallest_avg_age_5(self) -> list[dict]:
        """
        Retrieve the 5 rooms with the smallest average age of students.

        Returns:
            list[dict]: List of 5 dictionaries representing rooms with the lowest average age.
        """

        sql = """SELECT r.name, FLOOR(AVG((EXTRACT(YEAR FROM AGE(s.birthday))))) as avg_age
                 FROM rooms r
                          INNER JOIN students s ON r.id = s.room
                 GROUP BY r.name
                 ORDER BY avg_age
                 LIMIT 5;
              """
        return self.db.fetch(sql)

    def _largest_age_dif_5(self) -> list[dict]:
        """
        Retrieve the 5 rooms with the largest difference in student ages.

        Returns:
            list[dict]: List of 5 dictionaries representing rooms with the largest age differences.
        """

        sql = """SELECT r.name,
                        MAX(EXTRACT(YEAR FROM AGE(s.birthday))) - MIN(EXTRACT(YEAR FROM AGE(s.birthday))) as age_dif
                 FROM rooms r
                          INNER JOIN students s ON r.id = s.room
                 GROUP BY r.name
                 ORDER BY age_dif DESC
                 LIMIT 5;
              """
        return self.db.fetch(sql)

    def _rooms_dif_sex(self) -> list[dict]:
        """
        Retrieve a list of rooms that have students of different sexes.

        Returns:
            list[dict]: List of dictionaries, each representing a room with mixed-sex students.
        """

        sql = """SELECT r.name
                 FROM rooms r
                          INNER JOIN students s ON r.id = s.room
                 GROUP BY r.name
                 HAVING COUNT(DISTINCT s.sex) > 1;
              """
        return self.db.fetch(sql)

    def fetch_all_queries(self) -> dict:
        """
        Fetch all analytical queries related to rooms and students
        and aggregates their results into a single dictionary.

        Returns:
            dict: A dictionary where each key represents a query name and
                 the corresponding value contains the query result fetched from
                 the database.
        """

        return {
            "rooms_student_count": self._rooms_student_count(),
            "smallest_avg_age_5": self._smallest_avg_age_5(),
            "largest_age_dif_5": self._largest_age_dif_5(),
            "dif_sex_rooms": self._rooms_dif_sex(),
        }
