import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

from utils import generate_name, generate_email
from config import N_USERS

countries = [
    "US", "India", "UK", "Germany", "Vietnam",
    "Australia", "Canada", "Spain", "Brazil",
    "Singapore", "Japan", "France", "Other"
]

country_probs = [
    0.16, 0.12, 0.08, 0.07, 0.07,
    0.06, 0.06, 0.05, 0.05,
    0.04, 0.04, 0.04, 0.16
]

def generate_age(n):
    ages = np.random.normal(37, 10, n)
    return np.clip(ages, 18, 70).astype(int)

def generate_created_at(n):
    start = datetime(2023, 1, 1)
    end = datetime.now()

    days_range = (end - start).days
    weights = np.random.exponential(scale=1.5, size=n)
    weights /= weights.sum()

    random_days = (weights * days_range * n).astype(int) % days_range

    dates = []
    for d in random_days:
        base = start + timedelta(days=int(d))
        seconds = random.randint(0, 86399)
        dt = (base + timedelta(seconds=seconds)).replace(tzinfo=None)

        if dt.month in [1, 9, 11] and random.random() < 0.2:
            dt += timedelta(days=random.randint(-5, 5))

        dates.append(dt)

    return dates

def generate_users(n=N_USERS):
    user_ids = np.arange(1, n + 1)

    country_col = np.random.choice(countries, size=n, p=country_probs)
    age_col = generate_age(n)

    genders = ["Male", "Female", "Other"]
    gender_col = np.random.choice(genders, size=n, p=[0.54, 0.44, 0.02])

    channels = ["organic", "paid", "referral"]
    channel_col = np.random.choice(channels, size=n, p=[0.45, 0.35, 0.20])

    enterprise_col = np.random.choice([True, False], size=n, p=[0.18, 0.82])
    created_at_col = generate_created_at(n)

    first_names, last_names, emails = [], [], []

    for i in range(n):
        country = country_col[i]
        first, last = generate_name(country)

        first_names.append(first)
        last_names.append(last)

        if random.random() < 0.005:
            emails.append(None)
        else:
            emails.append(generate_email(first, last, country))

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
        "created_at": pd.to_datetime(created_at_col).tz_localize(None)
    })

    return df