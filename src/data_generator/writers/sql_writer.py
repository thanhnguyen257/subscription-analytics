# writers/sql_writer.py

import os
import pandas as pd
from config.settings import settings


def generate_insert_sql(df: pd.DataFrame, table_name: str, chunk_size=1000):

    columns = ",".join(df.columns)

    sql_chunks = []

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]

        values = []

        for row in chunk.itertuples(index=False):
            row_values = []

            for val in row:
                if pd.isna(val):
                    row_values.append("NULL")
                elif isinstance(val, str):
                    row_values.append(f"'{val}'")
                else:
                    row_values.append(str(val))

            values.append(f"({','.join(row_values)})")

        sql = f"INSERT INTO {table_name} ({columns}) VALUES\n" + ",\n".join(values) + ";"
        sql_chunks.append(sql)

    return "\n\n".join(sql_chunks)


def write_sql(df: pd.DataFrame, table_name: str):

    base_path = os.path.join(settings.OUTPUT_BASE, "sql")
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, f"{table_name}.sql")

    sql = generate_insert_sql(df, table_name)

    with open(file_path, "w") as f:
        f.write(sql)

    print(f"✅ SQL seed written: {file_path}")

    return file_path