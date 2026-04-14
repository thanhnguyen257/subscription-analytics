import pandas as pd
import numpy as np
import random
from datetime import timedelta
from config import N_PAYMENTS

PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "apple_pay", "google_pay", "invoice"]
METHOD_PROBS = [0.48, 0.18, 0.12, 0.08, 0.08, 0.06]

STATUS = ["success", "failed", "refunded"]
STATUS_PROBS = [0.92, 0.06, 0.02]

CURRENCY = ["USD", "EUR", "GBP", "VND"]
CURRENCY_PROBS = [0.5, 0.2, 0.1, 0.2]

def generate_payments(subs_df, plans_df, n=N_PAYMENTS):
    payments = []
    payment_id = 1

    plan_lookup = plans_df.set_index("plan_id").to_dict("index")

    for _ in range(n):
        sub = subs_df.sample(1).iloc[0]
        plan = plan_lookup[sub["plan_id"]]

        start = sub["start_date"]

        # payment date near renewal (bias to month start)
        days_offset = random.choice([0, 1, 2, 28, 29, 30])
        payment_date = start + timedelta(days=days_offset)

        # amount logic
        if plan["billing_cycle"] == "Monthly":
            amount = plan["price"]
        else:
            amount = plan["price"] * 12  # annual payment spike

        # enterprise skew
        if plan["tier"] == "Enterprise":
            amount *= random.uniform(1.0, 1.5)

        amount = round(amount, 2)

        status = np.random.choice(STATUS, p=STATUS_PROBS)

        # payment method bias
        if plan["tier"] == "Enterprise":
            method = np.random.choice(
                ["invoice", "bank_transfer", "credit_card"],
                p=[0.5, 0.3, 0.2]
            )
        else:
            method = np.random.choice(PAYMENT_METHODS, p=METHOD_PROBS)

        currency = np.random.choice(CURRENCY, p=CURRENCY_PROBS)

        payments.append((
            payment_id,
            sub["subscription_id"],
            amount,
            currency,
            status,
            payment_date,
            method
        ))

        payment_id += 1

    return pd.DataFrame(payments, columns=[
        "payment_id", "subscription_id", "amount",
        "currency", "payment_status",
        "payment_date", "payment_method"
    ])