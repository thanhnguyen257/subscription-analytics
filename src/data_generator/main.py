from users import generate_users
from products import generate_products
import os

OUTPUT_DIR = "data/master_db"

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    users_df = generate_users()
    products_df = generate_products()

    users_df.to_csv(f"{OUTPUT_DIR}/users.csv", index=False)
    products_df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)

    print("Data generated successfully!")
    print(f"- Users: {len(users_df)}")
    print(f"- Products: {len(products_df)}")

if __name__ == "__main__":
    main()