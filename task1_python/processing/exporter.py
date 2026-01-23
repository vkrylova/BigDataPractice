import json
import xml.etree.ElementTree as ET
from decimal import Decimal
from typing import Dict, List, Optional, Literal


class Exporter:
    """
    Handles exporting query results to JSON or XML.
    """

    @staticmethod
    def export(data_to_export: Dict[str, List[dict]],
               format: Literal["json", "xml"], file_path: Optional[str] = None) -> None:
        """
        Export query results to JSON or XML.

        Args:
            data_to_export (dict): Dictionary where keys are query names
                and values are lists of row dictionaries.
            format (str): Output format: 'json' or 'xml'.
            file_path (str, optional): File path to save output. If None, prints to stdout.

        Raises:
            ValueError: If an unsupported format is provided.
        """

        if not data_to_export:
            raise ValueError("No data to export")
        if format.lower() == "json":
            output = Exporter._to_json(data_to_export)
        elif format.lower() == "xml":
            output = Exporter._to_xml(data_to_export)
        else:
            raise ValueError(f"Unsupported export format: {format}. Use 'json' or 'xml' instead.")

        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(output)
        else:
            print(output)

    @staticmethod
    def _to_xml(data_to_export: Dict[str, List[dict]]) -> str:
        """
        Convert query results into an XML string.

        Args:
            data_to_export (dict): Dictionary where keys are query names and
                values are lists of row dictionaries representing query results.

        Returns:
            str: A string containing the XML representation of the data.
        """

        root = ET.Element("results")
        for key, rows in data_to_export.items():
            query_elem = ET.SubElement(root, key)
            for row in rows:
                row_elem = ET.SubElement(query_elem, "row")
                for k, v in row.items():
                    col_elem = ET.SubElement(row_elem, k)
                    col_elem.text = str(v)
        return ET.tostring(root, encoding="unicode")

    @staticmethod
    def _to_json(data_to_export: Dict[str, List[dict]]) -> str:
        """
        Convert query results into a JSON-formatted string.

        Args:
            data_to_export (dict): Dictionary where keys are query names and
                values are lists of row dictionaries representing query results.

        Returns:
            str: A pretty-printed JSON string of the data. Decimal values
                are automatically converted to floats.

        Raises:
            TypeError: If a value in the data is not serializable to JSON
                (other than Decimal).
        """

        def default(o):
            if isinstance(o, Decimal):
                return float(o)
            raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

        return json.dumps(data_to_export, indent=4, default=default)
