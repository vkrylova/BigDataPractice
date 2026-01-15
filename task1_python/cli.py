from db import DatabaseClient
import argparse
from loaders import StudentsLoader, RoomsLoader
from schema import SchemaManager
from queries import QueryService
from exporter import Exporter


class CLI:
    def __init__(self) -> None:
        self.parser = argparse.ArgumentParser(description="Students and Rooms loader")

        self.parser.add_argument(
            "--rooms",
            required=True,
            help="Path to the rooms file"
        )

        self.parser.add_argument(
            "--students",
            required=True,
            help="Path to the students file"
        )

        self.parser.add_argument(
            "--format",
            required=True,
            choices=("json", "xml"),
            help="Output format"
        )

        self.parser.add_argument(
            "--output",
            required=True,
            help="Output name without extension"
        )

    def run(self) -> None:
        args = self.parser.parse_args()

        db = DatabaseClient()
        try:
            db.connect()
            self._prepare_schema(db)
            self._load_data(db, args)
            data_to_upload = self._execute_queries(db)
            self._export(data_to_upload, args)
        finally:
            db.close()

    def _prepare_schema(self, db) -> None:
        SchemaManager(db).create_tables()

    def _load_data(self, db, args) -> None:
        RoomsLoader(db).load(args.rooms)
        StudentsLoader(db).load(args.students)
        db.conn.commit()

    def _execute_queries(self, db) -> dict:
        qs = QueryService(db)
        qs.create_indexes()
        return qs.run_all_queries()

    def _export(self, data_to_upload, args) -> None:
        output_file = f"{args.output}.{args.format}"
        Exporter.export(
            data=data_to_upload,
            format=args.format,
            file_path=output_file,
        )
