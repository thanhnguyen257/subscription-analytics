# generators/support_tickets.py

import numpy as np
import pandas as pd
import os

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator
from core.time_utils import filter_incremental

from config.distributions import (
    TICKET_CATEGORIES, TICKET_CATEGORY_PROBS
)


class SupportTicketsGenerator(BaseGenerator):

    def __init__(self, users_df, subs_df, payments_df, data_path="data/support_ticket_data"):
        super().__init__("support_tickets")
        self.users_df = users_df
        self.subs_df = subs_df
        self.payments_df = payments_df
        self.id_alloc = IDAllocator("tickets")

        # load once (IMPORTANT)
        self.text_map = self._load_and_map_kaggle(data_path)

        # flatten fallback pool
        self.all_texts = np.array(
            sum(self.text_map.values(), [])
        ) if any(self.text_map.values()) else np.array([])

    # =====================================================
    # MAIN GENERATOR
    # =====================================================
    def _generate(self):

        n = self.n

        ticket_ids = np.array(list(self.id_alloc.next_batch(n)))

        # =========================
        # USER SAMPLING (VECTOR)
        # =========================
        user_ids = np.random.choice(
            self.users_df["user_id"].values,
            size=n
        )

        users = self.users_df.set_index("user_id").loc[user_ids].reset_index()

        # =========================
        # CATEGORY
        # =========================
        category = np.random.choice(
            TICKET_CATEGORIES,
            size=n,
            p=TICKET_CATEGORY_PROBS
        )

        # =========================
        # BASE DATE
        # =========================
        created = users["created_at"] + pd.to_timedelta(
            np.random.randint(1, 1000, n),
            unit="D"
        )

        # =========================
        # BUSINESS CORRELATION
        # =========================
        # billing → failed payments
        billing_mask = category == "billing"

        failed_payments = self.payments_df[
            self.payments_df["payment_status"].isin(["failed", "refunded"])
        ]

        if billing_mask.any() and not failed_payments.empty:
            sampled = failed_payments.sample(
                min(billing_mask.sum(), len(failed_payments))
            )

            created.loc[billing_mask] = sampled["payment_date"].values[:billing_mask.sum()]

        # cancellation → near subscription end
        cancel_mask = category == "cancellation"

        cancelled_subs = self.subs_df[
            self.subs_df["status"] == "cancelled"
        ]

        if cancel_mask.any() and not cancelled_subs.empty:
            sampled = cancelled_subs.sample(
                min(cancel_mask.sum(), len(cancelled_subs))
            )

            created.loc[cancel_mask] = sampled["end_date"].fillna(
                sampled["start_date"]
            ).values[:cancel_mask.sum()]

        # =========================
        # DESCRIPTION (VECTORIZED + CATEGORY-AWARE)
        # =========================
        description = self._generate_descriptions(category)

        df = pd.DataFrame({
            "ticket_id": ticket_ids,
            "user_id": user_ids,
            "category": category,
            "description": description,
            "created_at": created
        })

        df = filter_incremental(df)

        return df

    # =====================================================
    # DESCRIPTION GENERATION
    # =====================================================
    def _generate_descriptions(self, categories):

        n = len(categories)
        descriptions = np.empty(n, dtype=object)

        for cat in TICKET_CATEGORIES:
            mask = categories == cat
            count = mask.sum()

            if count == 0:
                continue

            pool = self.text_map.get(cat, [])

            if pool:
                sampled = np.random.choice(pool, size=count, replace=True)
            elif len(self.all_texts) > 0:
                sampled = np.random.choice(self.all_texts, size=count, replace=True)
            else:
                sampled = np.array(["No description available"] * count)

            descriptions[mask] = sampled

        return descriptions

    # =====================================================
    # LOAD KAGGLE DATA (ONE TIME)
    # =====================================================
    def _load_and_map_kaggle(self, data_path):

        category_map = {
            "technical": [],
            "billing": [],
            "cancellation": [],
            "feature_request": []
        }

        if not os.path.exists(data_path):
            return category_map

        files = [
            os.path.join(data_path, f)
            for f in os.listdir(data_path)
            if f.endswith(".csv")
        ]

        for path in files:
            try:
                df = pd.read_csv(path)

                cols = {c.lower(): c for c in df.columns}

                desc_col = next((cols[c] for c in cols if "description" in c), None)
                cat_col = next((cols[c] for c in cols if "category" in c or "type" in c), None)

                if not desc_col:
                    continue

                desc_series = df[desc_col].astype(str).str.strip()

                if cat_col:
                    cat_series = df[cat_col].astype(str).str.lower()
                else:
                    cat_series = pd.Series([""] * len(df))

                for desc, cat_val in zip(desc_series, cat_series):

                    if any(k in cat_val for k in ["technical", "bug", "error", "account", "network"]):
                        category_map["technical"].append(desc)

                    elif any(k in cat_val for k in ["billing", "payment", "refund"]):
                        category_map["billing"].append(desc)

                    elif "cancel" in cat_val:
                        category_map["cancellation"].append(desc)

                    elif any(k in cat_val for k in ["feature", "product", "inquiry"]):
                        category_map["feature_request"].append(desc)

                    else:
                        category_map["feature_request"].append(desc)

            except Exception:
                continue

        return category_map