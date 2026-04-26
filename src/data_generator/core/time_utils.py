# core/time_utils.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config.settings import settings


def random_timestamp(start: datetime, end: datetime, size: int):
    delta = int((end - start).total_seconds())

    random_seconds = np.random.randint(0, delta, size)

    return pd.to_datetime([
        start + timedelta(seconds=int(s))
        for s in random_seconds
    ])


def ensure_temporal_consistency(df, start_col, end_col):
    """
    Ensure end >= start
    """
    mask = df[end_col] < df[start_col]
    df.loc[mask, end_col] = df.loc[mask, start_col]
    return df


def add_audit_columns(df: pd.DataFrame) -> pd.DataFrame:

    if "created_at" not in df.columns:
        raise ValueError("created_at must exist before audit columns")

    created = pd.to_datetime(df["created_at"])

    # updated_at >= created_at
    updated = created + pd.to_timedelta(
        np.random.randint(0, 30, len(df)),
        unit="D"
    )

    df["updated_at"] = updated

    # soft delete
    is_deleted = np.random.rand(len(df)) < settings.SOFT_DELETE_RATIO
    df["is_deleted"] = is_deleted

    return df


def filter_incremental(df: pd.DataFrame) -> pd.DataFrame:

    if settings.MODE != "incremental":
        return df

    if settings.EXECUTION_DATE:
        start = pd.to_datetime(settings.EXECUTION_DATE)
        end = start + pd.Timedelta(days=1)

    elif settings.EXECUTION_MONTH:
        start = pd.to_datetime(settings.EXECUTION_MONTH + "-01")
        end = start + pd.offsets.MonthEnd(1)

    else:
        raise ValueError("Incremental mode requires date or month")

    return df[
        (df["created_at"] >= start) &
        (df["created_at"] < end)
    ]