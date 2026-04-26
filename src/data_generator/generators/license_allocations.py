# generators/license_allocations.py

import numpy as np
import pandas as pd

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator

from config.distributions import (
    LICENSE_STATUS, LICENSE_STATUS_PROBS
)

class LicenseAllocationsGenerator(BaseGenerator):

    def __init__(self, licenses_df):
        super().__init__("allocations")
        self.licenses_df = licenses_df
        self.id_alloc = IDAllocator("allocations")

    def _generate(self):

        lic = self.licenses_df.copy()

        # utilization
        utilization = np.random.uniform(0.4, 0.9, len(lic))
        used_seats = (lic["max_seats"] * utilization).astype(int)

        # explode
        repeated_ids = np.repeat(lic["license_id"].values, used_seats)
        issued = np.repeat(lic["issued_date"].values, used_seats)
        expiry = np.repeat(lic["expiry_date"].values, used_seats)

        total = len(repeated_ids)

        # cap by config
        total = min(total, self.n)

        allocation_ids = np.array(list(self.id_alloc.next_batch(total)))

        repeated_ids = repeated_ids[:total]
        issued = issued[:total]
        expiry = expiry[:total]

        # seat numbers per license
        seat_numbers = np.concatenate([
            np.arange(1, s + 1) for s in used_seats
        ])[:total]

        status = np.random.choice(
            LICENSE_STATUS,
            size=total,
            p=LICENSE_STATUS_PROBS
        )

        duration = (expiry - issued).astype("timedelta64[D]").astype(int)

        allocation_date = issued + pd.to_timedelta(
            np.random.randint(0, duration.clip(min=1)),
            unit="D"
        )

        df = pd.DataFrame({
            "allocation_id": allocation_ids,
            "license_id": repeated_ids,
            "seat_number": seat_numbers,
            "status": status,
            "allocation_date": allocation_date,
            "created_at": allocation_date
        })

        return df