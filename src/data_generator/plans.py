import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

TIERS = (
    ["Free Trial"] * 2 +
    ["Basic"] * 6 +
    ["Standard"] * 5 +
    ["Premium"] * 5 +
    ["Enterprise"] * 2
)

CURRENCY_CHOICES = ["USD", "EUR", "GBP", "VND"]
CURRENCY_PROBS = [0.5, 0.2, 0.1, 0.2]

def get_price(tier):
    if tier == "Free Trial":
        return 0
    elif tier == "Basic":
        return random.uniform(5, 15)
    elif tier == "Standard":
        return random.uniform(12, 30)
    elif tier == "Premium":
        return random.uniform(25, 80)
    elif tier == "Enterprise":
        return random.uniform(150, 1200)

def generate_plans(products_df):
    plan_rows = []
    plan_id = 1

    product_ids = products_df["product_id"].tolist()

    # distribute plans across products
    product_plan_counts = {pid: random.choice([2,3,3,4]) for pid in product_ids}

    tier_pool = TIERS.copy()
    random.shuffle(tier_pool)

    for pid in product_ids:
        for _ in range(product_plan_counts[pid]):
            if not tier_pool:
                break

            tier = tier_pool.pop()

            billing_cycle = np.random.choice(
                ["Monthly", "Annual"], p=[0.7, 0.3]
            )

            price = round(get_price(tier), 2)

            currency = np.random.choice(
                CURRENCY_CHOICES, p=CURRENCY_PROBS
            )

            created_at = datetime(2021, 1, 1) + timedelta(
                days=random.randint(0, 365*3)
            )

            plan_rows.append((
                plan_id, pid, tier, billing_cycle,
                price, currency, created_at
            ))

            plan_id += 1

    df = pd.DataFrame(plan_rows, columns=[
        "plan_id", "product_id", "tier",
        "billing_cycle", "price", "currency", "created_at"
    ])

    return df