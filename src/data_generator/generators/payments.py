# generators/payments.py

import numpy as np
import pandas as pd

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator
from core.time_utils import filter_incremental

from config.distributions import (
    PAYMENT_METHODS, PAYMENT_METHOD_PROBS,
    PAYMENT_STATUS, PAYMENT_STATUS_PROBS,
    CURRENCY, CURRENCY_PROBS
)


class PaymentsGenerator(BaseGenerator):

    def __init__(self, subs_df, plans_df):
        super().__init__("payments")
        self.subs_df = subs_df.reset_index(drop=True)
        self.plans_df = plans_df.set_index("plan_id")
        self.id_alloc = IDAllocator("payments")

    def _generate(self):

        n = self.n

        payment_ids = np.array(list(self.id_alloc.next_batch(n)))

        # =========================
        # SAMPLE SUBSCRIPTIONS
        # =========================
        idx = np.random.randint(0, len(self.subs_df), n)
        subs = self.subs_df.iloc[idx].reset_index(drop=True)

        plans = self.plans_df.loc[subs["plan_id"]].reset_index()

        # =========================
        # PAYMENT DATE (renewal bias)
        # =========================
        offsets = np.random.choice([0, 1, 2, 28, 29, 30], size=n)

        payment_date = subs["start_date"] + pd.to_timedelta(offsets, unit="D")

        # =========================
        # AMOUNT
        # =========================
        monthly_amount = np.where(
            plans["billing_cycle"] == "Monthly",
            plans["price"],
            plans["price"] * 12
        )

        # enterprise skew
        enterprise_mask = plans["tier"] == "Enterprise"
        multiplier = np.ones(n)
        multiplier[enterprise_mask] = np.random.uniform(1.0, 1.5, enterprise_mask.sum())

        amount = np.round(monthly_amount * multiplier, 2)

        # =========================
        # STATUS
        # =========================
        status = np.random.choice(
            PAYMENT_STATUS,
            size=n,
            p=PAYMENT_STATUS_PROBS
        )

        # =========================
        # METHOD (conditional)
        # =========================
        method = np.random.choice(
            PAYMENT_METHODS,
            size=n,
            p=PAYMENT_METHOD_PROBS
        )

        # enterprise override
        enterprise_methods = ["invoice", "bank_transfer", "credit_card"]
        enterprise_probs = [0.5, 0.3, 0.2]

        override_idx = np.where(enterprise_mask)[0]

        method[override_idx] = np.random.choice(
            enterprise_methods,
            size=len(override_idx),
            p=enterprise_probs
        )

        currency = np.random.choice(
            CURRENCY,
            size=n,
            p=CURRENCY_PROBS
        )

        df = pd.DataFrame({
            "payment_id": payment_ids,
            "subscription_id": subs["subscription_id"],
            "amount": amount,
            "currency": currency,
            "payment_status": status,
            "payment_date": payment_date,
            "payment_method": method,
            "created_at": payment_date
        })

        df = filter_incremental(df)

        return df