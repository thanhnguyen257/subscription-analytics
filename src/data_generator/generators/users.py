# generators/users.py

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from functools import lru_cache

from core.base_generator import BaseGenerator
from core.id_allocator import IDAllocator
from core.time_utils import filter_incremental

from config.distributions import (
    COUNTRIES, COUNTRY_PROBS,
    GENDERS, GENDER_PROBS,
    ACQUISITION_CHANNELS, ACQUISITION_PROBS
)

faker_locale_map = {
    "US": "en_US",
    "India": "en_IN",
    "UK": "en_GB",
    "Germany": "de_DE",
    "Vietnam": "vi_VN",
    "Australia": "en_AU",
    "Canada": "en_CA",
    "Spain": "es_ES",
    "Brazil": "pt_BR",
    "Japan": "ja_JP",
    "France": "fr_FR",
    "Other": "en_US"
}

domain_map = {
    "US": ["gmail.com", "yahoo.com", "outlook.com"],
    "India": ["gmail.com", "yahoo.in"],
    "UK": ["gmail.co.uk", "hotmail.co.uk"],
    "Germany": ["gmail.de", "web.de"],
    "Vietnam": ["gmail.com", "yahoo.com.vn"],
    "Australia": ["gmail.com"],
    "Canada": ["gmail.com"],
    "Spain": ["gmail.es"],
    "Brazil": ["gmail.com.br"],
    "Singapore": ["gmail.com"],
    "Japan": ["gmail.co.jp"],
    "France": ["gmail.fr"],
    "Other": ["gmail.com"]
}


@lru_cache(maxsize=None)
def get_faker(locale):
    return Faker(locale)

class UsersGenerator(BaseGenerator):

    def __init__(self):
        super().__init__("users")
        self.id_alloc = IDAllocator("users")

    def _generate(self):

        n = self.n

        user_ids = np.array(list(self.id_alloc.next_batch(n)))

        country_col = np.random.choice(COUNTRIES, size=n, p=COUNTRY_PROBS)

        age_col = np.clip(
            np.random.normal(37, 10, n),
            18, 70
        ).astype(int)

        gender_col = np.random.choice(GENDERS, size=n, p=GENDER_PROBS)

        channel_col = np.random.choice(
            ACQUISITION_CHANNELS,
            size=n,
            p=ACQUISITION_PROBS
        )

        enterprise_col = np.random.choice(
            [True, False],
            size=n,
            p=[0.18, 0.82]
        )

        created_at = self._generate_created_at(n)

        first_names, last_names = self._generate_names(country_col)

        emails = self._generate_emails(
            first_names,
            last_names,
            country_col
        )

        df = pd.DataFrame({
            "user_id": user_ids,
            "first_name": first_names,
            "last_name": last_names,
            "email": emails,
            "country": country_col,
            "age": age_col,
            "gender": gender_col,
            "acquisition_channel": channel_col,
            "is_enterprise": enterprise_col,
            "created_at": created_at
        })

        df = filter_incremental(df)

        return df

    def _generate_created_at(self, n):

        start = datetime(2023, 1, 1)
        end = datetime.now()

        days_range = (end - start).days

        weights = np.random.exponential(scale=1.5, size=n)
        weights /= weights.sum()

        random_days = (weights * days_range * n).astype(int) % days_range

        base_dates = np.array([
            start + timedelta(days=int(d))
            for d in random_days
        ], dtype="datetime64[ns]")

        seconds = np.random.randint(0, 86400, n)

        created = base_dates + seconds.astype("timedelta64[s]")

        created = pd.to_datetime(created).tz_localize(None)

        # seasonal spikes
        months = created.month.values

        spike_mask = np.isin(months, [1, 9, 11]) & (np.random.rand(n) < 0.2)

        shift_days = np.random.randint(-5, 6, n)

        created = created + pd.to_timedelta(
            spike_mask * shift_days,
            unit="D"
        )

        return created

    def _generate_names(self, country_col):

        n = len(country_col)

        first_names = np.empty(n, dtype=object)
        last_names = np.empty(n, dtype=object)

        unique_countries = np.unique(country_col)

        for country in unique_countries:
            mask = country_col == country
            size = mask.sum()

            locale = faker_locale_map.get(country, "en_US")
            fake = get_faker(locale)

            first_names[mask] = [fake.first_name() for _ in range(size)]
            last_names[mask] = [fake.last_name() for _ in range(size)]

        return first_names, last_names

    def _generate_emails(self, first_names, last_names, country_col):

        n = len(first_names)

        domains = np.array([
            np.random.choice(domain_map[c])
            for c in country_col
        ])

        first_names = np.array(first_names, dtype=str)
        last_names = np.array(last_names, dtype=str)

        base = np.char.lower(
            np.char.add(
                np.char.add(first_names, "."),
                last_names
            )
        )

        base = np.char.replace(base, " ", "")

        suffix = np.arange(n).astype(str)

        emails = np.char.add(
            np.char.add(base, suffix),
            "@"
        )

        emails = np.char.add(emails, domains)

        emails = emails.astype(object)

        # 0.5% null emails
        null_mask = np.random.rand(n) < 0.005
        emails[null_mask] = None

        return emails