import pyodbc
import os


def update_db() -> None:
    """Restore the Latest 'Monthly' SQL Server database from a backup file."""

    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=master;"
        "Trusted_Connection=yes;"
    )
    # Very crucial for ALTER statement to work
    conn.autocommit = True

    def execute(query: str) -> None:
        """Helper to execute a query and consume all result sets."""
        with conn.cursor() as cursor:
            cursor.execute(query)
            while cursor.nextset():
                pass

    try:
        # Take database offline if it exists
        execute("""
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Monthly')
        BEGIN
            ALTER DATABASE [Monthly] SET OFFLINE WITH ROLLBACK IMMEDIATE;
        END
        """)
        print("Database set offline. Starting restore...")

        # Restore database
        execute(r"""
        RESTORE DATABASE [Monthly] FROM DISK = 'C:\bak\latest\Monthly.bak'
        WITH
            MOVE 'MonthlyBackup' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\MonthlyBackup.mdf',
            MOVE 'MonthlyBackup_log' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\MonthlyBackup_log.ldf',
            MOVE 'MonthlyBackup_2' TO 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\MonthlyBackup_2',
            REPLACE;
        """)
        print("Database restore completed successfully.")

        # Bring database online
        execute("ALTER DATABASE [Monthly] SET ONLINE;")
        print("Database is online and ready to use.")

    except Exception as e:
        print("An error occurred. Database restore not successful.")
        print("Error:", e)
    finally:
        conn.close()
