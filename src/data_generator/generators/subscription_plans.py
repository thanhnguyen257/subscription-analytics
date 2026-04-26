# generators/subscription_plans.py

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator

from config.distributions import (
    TIERS, CURRENCY, CURRENCY_PROBS
)


class SubscriptionPlansGenerator(BaseGenerator):

    def __init__(self, products_df):
        super().__init__("plans")
        self.products_df = products_df
        self.id_alloc = IDAllocator("plans")

    def _generate(self):

        product_ids = self.products_df["product_id"].values

        plan_counts = np.random.choice([2, 3, 3, 4], size=len(product_ids))

        total_plans = plan_counts.sum()

        plan_ids = np.array(list(self.id_alloc.next_batch(total_plans)))

        # expand product ids
        product_col = np.repeat(product_ids, plan_counts)

        # shuffle tiers
        tiers = np.random.choice(TIERS, size=total_plans, replace=True)

        billing_cycle = np.random.choice(
            ["Monthly", "Annual"],
            size=total_plans,
            p=[0.7, 0.3]
        )

        currency = np.random.choice(
            CURRENCY,
            size=total_plans,
            p=CURRENCY_PROBS
        )

        price = self._generate_prices(tiers)

        created_at = pd.to_datetime(
            datetime(2021, 1, 1)
        ) + pd.to_timedelta(
            np.random.randint(0, 365 * 3, total_plans),
            unit="D"
        )

        df = pd.DataFrame({
            "plan_id": plan_ids,
            "product_id": product_col,
            "tier": tiers,
            "billing_cycle": billing_cycle,
            "price": price,
            "currency": currency,
            "created_at": created_at
        })

        return df

    def _generate_prices(self, tiers):

        n = len(tiers)
        price = np.zeros(n)

        mask = tiers == "Basic"
        price[mask] = np.random.uniform(5, 15, mask.sum())

        mask = tiers == "Standard"
        price[mask] = np.random.uniform(12, 30, mask.sum())

        mask = tiers == "Premium"
        price[mask] = np.random.uniform(25, 80, mask.sum())

        mask = tiers == "Enterprise"
        price[mask] = np.random.uniform(150, 1200, mask.sum())

        return np.round(price, 2)