from db import DatabaseClient


class CLI:
    def __init__(self) -> None:
        pass

    def run(self) -> None:
        db = DatabaseClient()
        db.connect()
        db.close()
