from users import generate_users
from products import generate_products
from plans import generate_plans
from subscriptions import generate_subscriptions
from changes import generate_changes
from payments import generate_payments
from licenses import generate_licenses
from allocations import generate_allocations
from support_tickets import generate_support_tickets

import os

OUTPUT_DIR = "data/master_db"

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    users_df = generate_users()
    products_df = generate_products()
    plans_df = generate_plans(products_df)
    subs_df = generate_subscriptions(users_df, plans_df)
    changes_df = generate_changes(subs_df, plans_df)
    payments_df = generate_payments(subs_df, plans_df)
    licenses_df = generate_licenses(subs_df, plans_df)
    allocations_df = generate_allocations(licenses_df)
    tickets_df = generate_support_tickets(
        users_df,
        subs_df,
        payments_df,
        kaggle_paths=[
            "data/support_ticket_data/customer_support_tickets_1.csv",
            "data/support_ticket_data/customer_support_tickets_2.csv"
        ]
    )

    users_df.to_csv(f"{OUTPUT_DIR}/users.csv", index=False)
    products_df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
    plans_df.to_csv(f"{OUTPUT_DIR}/plans.csv", index=False)
    subs_df.to_csv(f"{OUTPUT_DIR}/subscriptions.csv", index=False)
    changes_df.to_csv(f"{OUTPUT_DIR}/changes.csv", index=False)
    payments_df.to_csv(f"{OUTPUT_DIR}/payments.csv", index=False)
    licenses_df.to_csv(f"{OUTPUT_DIR}/licenses.csv", index=False)
    allocations_df.to_csv(f"{OUTPUT_DIR}/allocations.csv", index=False)
    tickets_df.to_csv(f"{OUTPUT_DIR}/support_tickets.csv", index=False)

    print("Data generated successfully!")

if __name__ == "__main__":
    main()