# main.py

import argparse

from config.settings import settings

from generators.users import UsersGenerator
from generators.products import ProductsGenerator
from generators.subscription_plans import SubscriptionPlansGenerator
from generators.subscriptions import SubscriptionsGenerator
from generators.subscription_changes import SubscriptionChangesGenerator
from generators.payments import PaymentsGenerator
from generators.license_keys import LicenseKeysGenerator
from generators.license_allocations import LicenseAllocationsGenerator
from generators.support_tickets import SupportTicketsGenerator

from relationships.churn_logic import (
    identify_risky_users,
    identify_payment_issues,
    apply_churn_bias,
    enforce_ticket_churn_link
)

from writers.csv_writer import write_csv
from writers.sql_writer import write_sql
from writers.blob_writer import BlobWriter
from writers.sql_server_writer import write_to_sql_server


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", required=True, choices=["full", "incremental"])
    parser.add_argument("--date")
    parser.add_argument("--month")

    args = parser.parse_args()

    settings.MODE = args.mode
    settings.EXECUTION_DATE = args.date
    settings.EXECUTION_MONTH = args.month

    print(f"Running mode={settings.MODE}")

    users = UsersGenerator().generate()
    print(f"Generated users: {len(users)}")

    products = ProductsGenerator().generate()
    print(f"Generated products: {len(products)}")

    plans = SubscriptionPlansGenerator(products).generate()
    print(f"Generated plans: {len(plans)}")

    subs = SubscriptionsGenerator(users, plans).generate()
    
    changes = SubscriptionChangesGenerator(subs, plans).generate()
    print(f"Generated changes: {len(changes)}")

    payments = PaymentsGenerator(subs, plans).generate()
    print(f"Generated payments: {len(payments)}")

    licenses = LicenseKeysGenerator(subs, plans).generate()
    print(f"Generated licenses: {len(licenses)}")

    allocations = LicenseAllocationsGenerator(licenses).generate()
    print(f"Generated allocations: {len(allocations)}")

    tickets = SupportTicketsGenerator(users, subs, payments).generate()

    risky_users = identify_risky_users(tickets)
    problematic_subs = identify_payment_issues(payments)

    subs = apply_churn_bias(subs, risky_users, problematic_subs)
    print(f"Generated subscriptions: {len(subs)}")

    tickets = enforce_ticket_churn_link(tickets, subs)
    print(f"Generated tickets: {len(tickets)}")

    blob = BlobWriter()

    users_path = write_csv(users, "users")
    products_sql = write_sql(products, "products")
    plans_sql = write_sql(plans, "subscription_plans")

    subs_path = write_csv(subs, "subscriptions")
    changes_path = write_csv(changes, "subscription_changes")
    payments_path = write_csv(payments, "payments")

    tickets_path = write_csv(tickets, "support_tickets")

    blob.upload_file(users_path, "users")
    blob.upload_file(subs_path, "subscriptions")
    blob.upload_file(changes_path, "subscription_changes")
    blob.upload_file(payments_path, "payments")
    blob.upload_file(tickets_path, "support_tickets")

    blob.upload_file(products_sql, "products")
    blob.upload_file(plans_sql, "subscription_plans")

    write_to_sql_server(subs, "subscriptions")
    write_to_sql_server(licenses, "license_keys")
    write_to_sql_server(allocations, "license_allocations")


if __name__ == "__main__":
    main()