import pandas as pd
import random
import numpy as np
from datetime import timedelta
from config import N_ALLOCATIONS

def generate_allocations(licenses_df, n=N_ALLOCATIONS):
    allocations = []
    allocation_id = 1

    for _, lic in licenses_df.iterrows():
        max_seats = lic["max_seats"]

        # utilization (not all seats used)
        used_seats = int(max_seats * random.uniform(0.4, 0.9))

        for seat in range(1, used_seats + 1):
            if allocation_id > n:
                break

            status = np.random.choice(
                ["active", "deactivated"], p=[0.72, 0.28]
            )

            allocation_date = lic["issued_date"] + timedelta(
                days=random.randint(0, (lic["expiry_date"] - lic["issued_date"]).days)
            )

            allocations.append((
                allocation_id,
                lic["license_id"],
                seat,
                status,
                allocation_date
            ))

            allocation_id += 1

    return pd.DataFrame(allocations, columns=[
        "allocation_id", "license_id",
        "seat_number", "status", "allocation_date"
    ])