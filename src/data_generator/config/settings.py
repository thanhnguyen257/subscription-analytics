# config/settings.py

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from dotenv import load_dotenv
import os

load_dotenv()

@dataclass
class Settings:
    MODE: str = "full"  # full | incremental
    EXECUTION_DATE: Optional[str] = None  # "YYYY-MM-DD"
    EXECUTION_MONTH: Optional[str] = None  # "YYYY-MM"

    RANDOM_SEED: int = 2001

    SCALE: Dict[str, int] = field(default_factory=lambda: {
        "users": 50_000,
        "subscriptions": 250_000,
        "changes": 25_000,
        "payments": 350_000,
        "licenses": 11_000,
        "allocations": 55_000,
        "support_tickets": 22_255
    })

    OUTPUT_BASE: str = "data/output"

    PARTITION_BY: List[str] = field(default_factory=lambda: [
        "year", "month"
    ])

    WRITE_MODE: str = "overwrite"  # overwrite | append

    ADD_AUDIT_COLUMNS: bool = False
    SOFT_DELETE_RATIO: float = 0.01  # 1% deleted rows

    AZURE_SQL_CONN_STR: str = os.getenv('AZURE_SQL_CONN_STR')
    ADLS_CONNECTION_STR: str = os.getenv('ADLS_CONNECTION_STR')
    BLOB_CONTAINER: str = "raw"
    STORAGE_ACCOUNT: str = "stanalyticsstorage"

# singleton
settings = Settings()