# writers/sql_server_writer.py

import pyodbc
from config.settings import settings


def map_type(dtype, col_name=None):
    dtype_str = str(dtype)

    # INTEGER
    if dtype_str == "int64":
        return "BIGINT"
    if "int" in dtype_str:
        return "INT"

    # BOOLEAN
    if dtype_str == "bool":
        return "BIT"

    # DATETIME
    if "datetime" in dtype_str:
        return "DATETIME2"

    # STRING / OBJECT
    if "object" in dtype_str or "str" in dtype_str:
        # optional smarter sizing rules
        if col_name and col_name.endswith("_id"):
            return "NVARCHAR(100)"
        return "NVARCHAR(255)"

    # fallback
    return "NVARCHAR(MAX)"

def table_exists(cursor, table_name: str) -> bool:
    cursor.execute("""
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
    """, table_name)

    return cursor.fetchone() is not None

def create_table(cursor, df, table_name: str):

    columns_sql = ", ".join(
        [f"[{col}] {map_type(dtype, col)}"
         for col, dtype in df.dtypes.items()]
    )

    sql = f"""
    CREATE TABLE {table_name} (
        {columns_sql}
    )
    """

    cursor.execute(sql)

def write_to_sql_server(df, table_name: str):

    conn = pyodbc.connect(settings.AZURE_SQL_CONN_STR)
    cursor = conn.cursor()

    try:
        # STEP 1: Create table if not exists
        if not table_exists(cursor, table_name):
            print(f"⚙️ Creating table: {table_name}")
            create_table(cursor, df, table_name)
            conn.commit()
            print(f"✅ Table created: {table_name}")

        # STEP 2: Prepare insert
        cols = ",".join([f"[{c}]" for c in df.columns])
        placeholders = ",".join(["?"] * len(df.columns))

        sql = f"""
        INSERT INTO {table_name} ({cols})
        VALUES ({placeholders})
        """

        # STEP 3: Insert data
        cursor.fast_executemany = True
        cursor.executemany(sql, df.values.tolist())

        conn.commit()

        print(f"✅ Inserted into Azure SQL: {table_name} ({len(df)} rows)")

    finally:
        cursor.close()
        conn.close()