# generators/license_keys.py

import numpy as np
import pandas as pd

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator


class LicenseKeysGenerator(BaseGenerator):

    def __init__(self, subs_df, plans_df):
        super().__init__("licenses")
        self.subs_df = subs_df
        self.plans_df = plans_df
        self.id_alloc = IDAllocator("licenses")

    def _generate(self):

        # enterprise plan ids
        enterprise_plans = self.plans_df[
            self.plans_df["tier"] == "Enterprise"
        ]["plan_id"].values

        subs = self.subs_df[
            self.subs_df["plan_id"].isin(enterprise_plans)
        ].reset_index(drop=True)

        n = min(self.n, len(subs))

        idx = np.random.choice(len(subs), n, replace=False)
        subs = subs.iloc[idx]

        # number of keys per subscription
        num_keys = np.where(
            np.random.rand(n) < 0.8,
            1,
            np.random.randint(2, 4, n)
        )

        total_keys = num_keys.sum()

        license_ids = np.array(list(self.id_alloc.next_batch(total_keys)))

        sub_ids = np.repeat(subs["subscription_id"].values, num_keys)
        start_dates = np.repeat(subs["start_date"].values, num_keys)

        issued = start_dates + pd.to_timedelta(
            np.random.randint(0, 10, total_keys),
            unit="D"
        )

        expiry = issued + pd.to_timedelta(
            np.random.choice([180, 365, 730], total_keys),
            unit="D"
        )

        seats = self._generate_seats(total_keys)

        df = pd.DataFrame({
            "license_id": license_ids,
            "subscription_id": sub_ids,
            "max_seats": seats,
            "issued_date": issued,
            "expiry_date": expiry,
            "created_at": issued
        })

        return df

    def _generate_seats(self, n):

        r = np.random.rand(n)

        seats = np.zeros(n, dtype=int)

        mask = r < 0.45
        seats[mask] = np.random.randint(5, 21, mask.sum())

        mask = (r >= 0.45) & (r < 0.75)
        seats[mask] = np.random.randint(21, 51, mask.sum())

        mask = (r >= 0.75) & (r < 0.95)
        seats[mask] = np.random.randint(51, 201, mask.sum())

        mask = r >= 0.95
        seats[mask] = np.random.randint(201, 501, mask.sum())

        return seats