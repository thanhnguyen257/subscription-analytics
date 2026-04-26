# generators/subscription_changes.py

import numpy as np
import pandas as pd

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator


class SubscriptionChangesGenerator(BaseGenerator):

    def __init__(self, subs_df, plans_df):
        super().__init__("changes")
        self.subs_df = subs_df.reset_index(drop=True)
        self.plans_df = plans_df
        self.id_alloc = IDAllocator("changes")

    def _generate(self):

        n = self.n

        change_ids = np.array(list(self.id_alloc.next_batch(n)))

        # sample subscriptions
        idx = np.random.randint(0, len(self.subs_df), n)
        subs = self.subs_df.iloc[idx].reset_index(drop=True)

        old_plan = subs["plan_id"].values

        new_plan = np.random.choice(
            self.plans_df["plan_id"].values,
            size=n
        )

        change_type = np.random.choice(
            ["upgrade", "downgrade", "initial"],
            size=n,
            p=[0.55, 0.25, 0.2]
        )

        start = subs["start_date"]
        end = subs["end_date"].fillna(
            start + pd.Timedelta(days=365)
        )

        duration = (end - start).dt.days.clip(lower=1)

        change_date = start + pd.to_timedelta(
            np.random.randint(0, duration),
            unit="D"
        )

        df = pd.DataFrame({
            "change_id": change_ids,
            "subscription_id": subs["subscription_id"],
            "old_plan_id": old_plan,
            "new_plan_id": new_plan,
            "change_type": change_type,
            "change_date": change_date,
            "created_at": change_date
        })

        return df