
import duckdb
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal


# MinIO S3 configuration
MINIO_CONFIG = {
    "endpoint": "localhost:9002",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "region": "us-east-1",
}


# SQL directory
SQL_DIR = Path(__file__).parent / "sql"


def get_connection():
    """
    Create and configure a DuckDB connection to MinIO.
    """
    con = duckdb.connect()

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID '{MINIO_CONFIG["access_key"]}',
            SECRET '{MINIO_CONFIG["secret_key"]}',
            REGION '{MINIO_CONFIG["region"]}',
            ENDPOINT '{MINIO_CONFIG["endpoint"]}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)

    return con


def execute_query(sql_file: str):
    """
    Execute a SQL file and return JSON-compatible results.
    """
    file_path = SQL_DIR / sql_file

    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    query = file_path.read_text(encoding="utf-8")

    con = get_connection()

    try:
        cursor = con.execute(query)

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        result = []

        for row in rows:
            record = dict(zip(columns, row))

            for key, value in record.items():
                if isinstance(value, (date, datetime)):
                    record[key] = value.isoformat()

                elif isinstance(value, Decimal):
                    record[key] = float(value)

            result.append(record)

        return result

    finally:
        con.close()
