# writers/csv_writer.py

import os
import pandas as pd
from config.settings import settings


# =========================
# FILE NAMING
# =========================
def _build_filename(table_name: str, year: int, month: int):
    return f"{table_name}_{year}_{month:02d}.csv"


# =========================
# MAIN WRITER
# =========================
def write_csv(df: pd.DataFrame, table_name: str):

    if "created_at" not in df.columns:
        raise ValueError(f"{table_name} missing created_at column")

    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"])

    # extract partitions (vectorized)
    df["year"] = df["created_at"].dt.year
    df["month"] = df["created_at"].dt.month

    base_path = os.path.join(settings.OUTPUT_BASE, table_name)
    os.makedirs(base_path, exist_ok=True)

    written_files = []

    # group by year-month (fast pandas groupby)
    grouped = df.groupby(["year", "month"])

    for (year, month), group in grouped:

        filename = _build_filename(table_name, year, month)
        file_path = os.path.join(base_path, filename)

        # drop partition columns before writing
        group = group.drop(columns=["year", "month"])

        # write (overwrite per partition)
        group.to_csv(file_path, index=False)

        written_files.append(file_path)

    print(f"✅ {table_name}: wrote {len(written_files)} partition files")

    return written_files