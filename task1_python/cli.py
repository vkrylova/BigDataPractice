from db.connection import DatabaseClient
import argparse
from processing.loaders import StudentsLoader, RoomsLoader
from db.schema import SchemaManager
from db.queries import QueryService
from processing.exporter import Exporter
from pathlib import Path


class CLI:
    """
    Command-line interface for loading students and rooms data,
    executing analytical queries, and exporting results.

    This class orchestrates the full application workflow:
    parsing command-line arguments, initializing the db,
    loading data, running queries, and exporting the output.
    """

    def __init__(self) -> None:
        """
        Initializes the command-line argument parser and
        defines all supported CLI options.
        """

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
        """
        Executes the full CLI workflow.
        """

        args = self.parser.parse_args()

        db = DatabaseClient()
        try:
            db.connect()
            self._prepare_schema(db)
            self._load_data(db, args)
            data_to_export = self._execute_queries(db)
            self._export(data_to_export, args)
        finally:
            db.close()

    def _prepare_schema(self, db) -> None:
        """
        Creates db tables required for the application.

        Args:
        db: Active db client instance.
        """

        SchemaManager(db).create_tables()

    def _load_data(self, db, args) -> None:
        """
        Loads rooms and students data into the db.

        Args:
            db: Active db client instance.
            args: Parsed command-line arguments containing input file paths
        """

        RoomsLoader(db).load(args.rooms)
        StudentsLoader(db).load(args.students)
        db.conn.commit()

    def _execute_queries(self, db) -> dict:
        """
        Executes analytical queries on the loaded data.

        Creates required indexes and retrieves aggregated
        query results from the db.

        Args:
            db: Active db client instance.

        Returns:
            dict: Aggregated query results keyed by query name.
        """

        qs = QueryService(db)
        qs.create_indexes()
        return qs.fetch_all_queries()

    def _export(self, data_to_export, args) -> None:
        """
        Exports query results to a file in the requested format.

        Args:
            data_to_export: Query results to export.
            args: Parsed command-line arguments containing output settings.
        """
        output_dir = Path("data/output")
        output_file = output_dir / f"{args.output}.{args.format}"
        Exporter.export(
            data_to_export=data_to_export,
            format=args.format,
            file_path=str(output_file),
        )
