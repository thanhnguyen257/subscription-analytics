import pandas as pd
import numpy as np
import random
from datetime import timedelta
from config import N_SUBSCRIPTION

def monthly_mrr(price, billing_cycle):
    return price if billing_cycle == "Monthly" else price / 12

def generate_subscriptions(users_df, plans_df, n=N_SUBSCRIPTION):
    subs = []

    sub_id = 1

    for _ in range(n):
        user = users_df.sample(1).iloc[0]
        plan = plans_df.sample(1).iloc[0]

        start_date = user["created_at"] + timedelta(
            days=random.randint(0, 365)
        )

        status = np.random.choice(
            ["active", "cancelled", "expired", "trial"],
            p=[0.6, 0.25, 0.1, 0.05]
        )

        end_date = None
        if status in ["cancelled", "expired"]:
            end_date = start_date + timedelta(
                days=random.randint(30, 365)
            )

        # MRR logic
        if plan["tier"] == "Free Trial" or status == "trial":
            mrr = 0
        else:
            mrr = monthly_mrr(plan["price"], plan["billing_cycle"])

        subs.append((
            sub_id,
            user["user_id"],
            plan["plan_id"],
            start_date,
            end_date,
            status,
            round(mrr, 2),
            start_date
        ))

        sub_id += 1

    df = pd.DataFrame(subs, columns=[
        "subscription_id", "user_id", "plan_id",
        "start_date", "end_date", "status",
        "current_mrr", "created_at"
    ])

    return df