from faker import Faker
from functools import lru_cache
import random

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
    # "Singapore": "en_SG",
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

def generate_name(country):
    locale = faker_locale_map.get(country, "en_US")
    fake = get_faker(locale)
    return fake.first_name(), fake.last_name()

used_emails = set()

def generate_email(first, last, country):
    domain = random.choice(domain_map[country])
    base = f"{first}.{last}".lower().replace(" ", "")
    email = f"{base}@{domain}"

    counter = 1
    while email in used_emails:
        email = f"{base}{counter}@{domain}"
        counter += 1

    used_emails.add(email)
    return email