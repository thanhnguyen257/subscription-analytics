import pyodbc
import pandas as pd
from config.settings import settings


def map_type(dtype, col_name=None):
    dtype_str = str(dtype)

    if dtype_str == "int64":
        return "BIGINT"
    if "int" in dtype_str:
        return "INT"

    if dtype_str == "bool":
        return "BIT"

    if "datetime" in dtype_str:
        return "DATETIME2"

    if "object" in dtype_str or "str" in dtype_str:
        if col_name and col_name.endswith("_id"):
            return "NVARCHAR(100)"
        return "NVARCHAR(255)"

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

def clean_dataframe(df):
    df = df.copy()

    obj_cols = df.select_dtypes(include=["object"]).columns

    for col in obj_cols:
        df[col] = df[col].map(
            lambda x: x.decode("utf-8", errors="ignore") if isinstance(x, bytes) else x
        )

    df = df.where(df.notna(), None)

    return df

def write_to_sql_server(df: pd.DataFrame, table_name: str, batch_size: int = 1000):

    conn = pyodbc.connect(settings.AZURE_SQL_CONN_STR)
    cursor = conn.cursor()

    try:
        df = clean_dataframe(df)

        if not table_exists(cursor, table_name):
            print(f"⚙️ Creating table: {table_name}")
            create_table(cursor, df, table_name)
            conn.commit()
            print(f"✅ Table created: {table_name}")

        cols = ",".join([f"[{c}]" for c in df.columns])
        placeholders = ",".join(["?"] * len(df.columns))

        sql = f"""
        INSERT INTO {table_name} ({cols})
        VALUES ({placeholders})
        """

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        cursor.fast_executemany = True

        total_rows = len(data)
        print(f"🚀 Inserting {total_rows} rows in batches of {batch_size}...")

        for i in range(0, total_rows, batch_size):
            batch = data[i:i + batch_size]

            try:
                cursor.executemany(sql, batch)
                conn.commit()
            except Exception as e:
                print(f"❌ Error in batch {i}-{i+batch_size}: {e}")

                # fallback: find bad row
                for row in batch:
                    try:
                        cursor.execute(sql, row)
                    except Exception as row_error:
                        print("💥 Bad row detected:", row)
                        raise row_error

                raise e

            print(f"✅ Inserted rows {i} → {min(i+batch_size, total_rows)}")

        print(f"🎉 Done: Inserted {total_rows} rows into {table_name}")

    finally:
        cursor.close()
        conn.close()