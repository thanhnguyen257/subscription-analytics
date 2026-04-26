# core/base_generator.py

import numpy as np
import pandas as pd
from config.settings import settings
from core.time_utils import add_audit_columns


class BaseGenerator:

    def __init__(self, name: str):
        self.name = name
        self.n = settings.SCALE.get(name, 0)

        # global seed control
        np.random.seed(settings.RANDOM_SEED)

    def generate(self) -> pd.DataFrame:
        """
        Main entrypoint for all generators
        """
        df = self._generate()

        if settings.ADD_AUDIT_COLUMNS:
            df = add_audit_columns(df)

        df = self._post_process(df)

        return df

    def _generate(self) -> pd.DataFrame:
        """
        Must be implemented by subclass
        """
        raise NotImplementedError

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optional hook
        """
        return df

    # =========================
    # HELPERS
    # =========================
    def sample_indices(self, size: int, max_val: int):
        return np.random.randint(0, max_val, size)

    def random_dates(self, start, end, size):
        delta = (end - start).days
        return start + pd.to_timedelta(
            np.random.randint(0, delta, size),
            unit="D"
        )