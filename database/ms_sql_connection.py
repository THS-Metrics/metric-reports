import pyodbc
import pandas as pd
from environment.settings import config


def fetch_query(query: str) -> pd.DataFrame:
    """Execute a SQL query using a fresh database connection."""

    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        f"Database={config.MS_SQL_DB};"
        "Trusted_Connection=yes;"
    )

    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()
