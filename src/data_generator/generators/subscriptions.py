# generators/subscriptions.py

import numpy as np
import pandas as pd

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator
from core.time_utils import filter_incremental

from config.distributions import (
    SUB_STATUS, SUB_STATUS_PROBS
)


class SubscriptionsGenerator(BaseGenerator):

    def __init__(self, users_df, plans_df):
        super().__init__("subscriptions")
        self.users_df = users_df.reset_index(drop=True)
        self.plans_df = plans_df.set_index("plan_id")
        self.id_alloc = IDAllocator("subscriptions")

    def _generate(self):

        n = self.n

        sub_ids = np.array(list(self.id_alloc.next_batch(n)))

        # =========================
        # SAMPLE USERS & PLANS (FAST)
        # =========================
        user_idx = np.random.randint(0, len(self.users_df), n)
        users = self.users_df.iloc[user_idx].reset_index(drop=True)

        plan_ids = np.random.choice(self.plans_df.index, size=n)
        plans = self.plans_df.loc[plan_ids].reset_index()

        # =========================
        # DATES
        # =========================
        start_date = users["created_at"] + pd.to_timedelta(
            np.random.randint(0, 365, n),
            unit="D"
        )

        status = np.random.choice(
            SUB_STATUS,
            size=n,
            p=SUB_STATUS_PROBS
        )

        # end_date logic
        end_date = pd.Series([pd.NaT] * n)

        mask = np.isin(status, ["cancelled", "expired"])

        end_date.loc[mask] = start_date.loc[mask] + pd.to_timedelta(
            np.random.randint(30, 365, mask.sum()),
            unit="D"
        )

        # =========================
        # MRR
        # =========================
        monthly_price = np.where(
            plans["billing_cycle"] == "Monthly",
            plans["price"],
            plans["price"] / 12
        )

        is_free = (plans["tier"] == "Free Trial") | (status == "trial")

        mrr = np.where(is_free, 0, monthly_price)

        df = pd.DataFrame({
            "subscription_id": sub_ids,
            "user_id": users["user_id"].values,
            "plan_id": plan_ids,
            "start_date": start_date,
            "end_date": end_date,
            "status": status,
            "current_mrr": np.round(mrr, 2),
            "created_at": start_date
        })

        df = filter_incremental(df)

        return df