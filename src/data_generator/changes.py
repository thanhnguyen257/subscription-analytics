import pandas as pd
import numpy as np
import random
from datetime import timedelta
from config import N_CHANGES

def generate_changes(subs_df, plans_df, n=N_CHANGES):
    changes = []
    change_id = 1

    subs_sample = subs_df.sample(n)

    for _, sub in subs_sample.iterrows():
        old_plan = sub["plan_id"]

        new_plan = plans_df.sample(1).iloc[0]["plan_id"]

        change_type = np.random.choice(
            ["upgrade", "downgrade", "initial"],
            p=[0.55, 0.25, 0.2]
        )

        start = sub["start_date"]
        end = sub["end_date"] if pd.notnull(sub["end_date"]) else start + timedelta(days=365)

        change_date = start + timedelta(
            days=random.randint(0, max(1, (end - start).days))
        )

        changes.append((
            change_id,
            sub["subscription_id"],
            old_plan,
            new_plan,
            change_type,
            change_date
        ))

        change_id += 1

    df = pd.DataFrame(changes, columns=[
        "change_id", "subscription_id",
        "old_plan_id", "new_plan_id",
        "change_type", "change_date"
    ])

    return df