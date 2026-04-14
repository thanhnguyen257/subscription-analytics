import pandas as pd
import random
from datetime import timedelta
from config import N_LICENSES

def generate_seats():
    r = random.random()
    if r < 0.45:
        return random.randint(5, 20)
    elif r < 0.75:
        return random.randint(21, 50)
    elif r < 0.95:
        return random.randint(51, 200)
    else:
        return random.randint(201, 500)

def generate_licenses(subs_df, plans_df, n=N_LICENSES):
    licenses = []
    license_id = 1

    plan_lookup = plans_df.set_index("plan_id").to_dict("index")

    # filter enterprise subscriptions
    enterprise_subs = subs_df[
        subs_df["plan_id"].isin(
            plans_df[plans_df["tier"] == "Enterprise"]["plan_id"]
        )
    ]

    sampled_subs = enterprise_subs.sample(min(n, len(enterprise_subs)))

    for _, sub in sampled_subs.iterrows():
        num_keys = 1 if random.random() < 0.8 else random.randint(2, 3)

        for _ in range(num_keys):
            issued = sub["start_date"] + timedelta(days=random.randint(0, 10))

            expiry = issued + timedelta(days=random.choice([365, 180, 730]))

            max_seats = generate_seats()

            licenses.append((
                license_id,
                sub["subscription_id"],
                max_seats,
                issued,
                expiry
            ))

            license_id += 1

    return pd.DataFrame(licenses, columns=[
        "license_id", "subscription_id",
        "max_seats", "issued_date", "expiry_date"
    ])