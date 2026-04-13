from users import generate_users
from products import generate_products
from plans import generate_plans
from subscriptions import generate_subscriptions

import os

OUTPUT_DIR = "data/master_db"

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    users_df = generate_users()
    products_df = generate_products()
    plans_df = generate_plans(products_df)
    subs_df = generate_subscriptions(users_df, plans_df)

    users_df.to_csv(f"{OUTPUT_DIR}/users.csv", index=False)
    products_df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
    plans_df.to_csv(f"{OUTPUT_DIR}/plans.csv", index=False)
    subs_df.to_csv(f"{OUTPUT_DIR}/subscriptions.csv", index=False)

    print("Data generated successfully!")

if __name__ == "__main__":
    main()