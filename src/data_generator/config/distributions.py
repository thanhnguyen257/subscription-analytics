# config/distributions.py

import numpy as np

# =========================
# USER DISTRIBUTIONS
# =========================
COUNTRIES = [
    "US", "India", "UK", "Germany", "Vietnam",
    "Australia", "Canada", "Spain", "Brazil",
    "Singapore", "Japan", "France", "Other"
]

COUNTRY_PROBS = [
    0.16, 0.12, 0.08, 0.07, 0.07,
    0.06, 0.06, 0.05, 0.05,
    0.04, 0.04, 0.04, 0.16
]

GENDERS = ["Male", "Female", "Other"]
GENDER_PROBS = [0.54, 0.44, 0.02]

ACQUISITION_CHANNELS = ["organic", "paid", "referral"]
ACQUISITION_PROBS = [0.45, 0.35, 0.20]


SUB_STATUS = ["active", "cancelled", "expired", "trial"]
SUB_STATUS_PROBS = [0.60, 0.25, 0.10, 0.05]


PAYMENT_STATUS = ["success", "failed", "refunded"]
PAYMENT_STATUS_PROBS = [0.92, 0.06, 0.02]

PAYMENT_METHODS = [
    "credit_card", "paypal", "bank_transfer",
    "apple_pay", "google_pay", "invoice"
]

PAYMENT_METHOD_PROBS = [0.48, 0.18, 0.12, 0.08, 0.08, 0.06]

TIERS = (
    ["Free Trial"] * 2 +
    ["Basic"] * 6 +
    ["Standard"] * 5 +
    ["Premium"] * 5 +
    ["Enterprise"] * 2
)

CURRENCY = ["USD", "EUR", "GBP", "VND"]
CURRENCY_PROBS = [0.5, 0.2, 0.1, 0.2]


LICENSE_STATUS = ["active", "deactivated"]
LICENSE_STATUS_PROBS = [0.72, 0.28]


TICKET_CATEGORIES = [
    "technical", "billing", "cancellation", "feature_request"
]

TICKET_CATEGORY_PROBS = [0.35, 0.30, 0.20, 0.15]


def choice(arr, probs, size):
    return np.random.choice(arr, size=size, p=probs)


def bernoulli(p, size):
    return np.random.rand(size) < p