"""
Configuration module for the Spark project.

This module loads environment variables and constructs the necessary
connection parameters for the Apache Spark cluster and the PostgreSQL database.
"""

import os

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")

JDBC_URL = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

DB_PROPS = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}
