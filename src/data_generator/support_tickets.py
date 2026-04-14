import pandas as pd
import numpy as np
import random
from datetime import timedelta
from config import N_SUPPORT_TICKETS

CATEGORIES = ["technical", "billing", "cancellation", "feature_request"]
CATEGORY_PROBS = [0.35, 0.30, 0.20, 0.15]

# def generate_text_length():
#     r = random.random()
#     if r < 0.2:
#         return random.randint(20, 80)
#     elif r < 0.75:
#         return random.randint(81, 250)
#     else:
#         return random.randint(251, 800)

def load_and_map_kaggle(file_paths):
    category_map = {
        "technical": [],
        "billing": [],
        "cancellation": [],
        "feature_request": []
    }

    for path in file_paths:
        try:
            df = pd.read_csv(path)

            cols = {c.lower(): c for c in df.columns}

            desc_col = None
            cat_col = None

            for c in cols:
                if "description" in c:
                    desc_col = cols[c]
                if "category" in c or "type" in c:
                    cat_col = cols[c]

            if not desc_col:
                continue

            for _, row in df.iterrows():
                desc = str(row[desc_col]).strip()
                cat_val = str(row[cat_col]).lower() if cat_col else ""

                if any(k in cat_val for k in ["technical", "bug", "error", "account", "network"]):
                    category_map["technical"].append(desc)

                elif any(k in cat_val for k in ["billing", "payment", "refund"]):
                    category_map["billing"].append(desc)

                elif any(k in cat_val for k in ["cancel"]):
                    category_map["cancellation"].append(desc)

                elif any(k in cat_val for k in ["inquiry", "feature", "product"]):
                    category_map["feature_request"].append(desc)

                else:
                    category_map["feature_request"].append(desc)

        except Exception:
            continue

    return category_map


def generate_support_tickets(
    users_df,
    subs_df,
    payments_df,
    n=N_SUPPORT_TICKETS,
    kaggle_paths=None
):
    tickets = []
    ticket_id = 1

    text_map = load_and_map_kaggle(kaggle_paths or [])

    user_ticket_counts = {}

    subs_by_user = subs_df.groupby("user_id")

    for _ in range(n):

        # ==============================
        # USER SELECTION (behavior bias)
        # ==============================
        if random.random() < 0.3 and user_ticket_counts:
            user_id = random.choice(list(user_ticket_counts.keys()))
        else:
            user_id = users_df.sample(1).iloc[0]["user_id"]

        user = users_df.loc[users_df["user_id"] == user_id].iloc[0]

        # ==============================
        # CATEGORY
        # ==============================
        category = np.random.choice(CATEGORIES, p=CATEGORY_PROBS)

        base_date = user["created_at"]
        ticket_date = base_date + timedelta(days=random.randint(1, 1000))

        user_subs = subs_by_user.get_group(user_id) if user_id in subs_by_user.groups else None

        # ==============================
        # BUSINESS CORRELATION
        # ==============================
        if category == "billing" and user_subs is not None:
            sub_ids = user_subs["subscription_id"].tolist()

            relevant = payments_df[
                (payments_df["subscription_id"].isin(sub_ids)) &
                (payments_df["payment_status"].isin(["failed", "refunded"]))
            ]

            if not relevant.empty:
                payment = relevant.sample(1).iloc[0]
                ticket_date = payment["payment_date"] + timedelta(days=random.randint(0, 3))

        elif category == "cancellation" and user_subs is not None:
            cancelled = user_subs[user_subs["status"] == "cancelled"]

            if not cancelled.empty:
                sub = cancelled.sample(1).iloc[0]
                if pd.notnull(sub["end_date"]):
                    ticket_date = sub["end_date"] - timedelta(days=random.randint(0, 5))

        # ==============================
        # DESCRIPTION (CATEGORY-AWARE)
        # ==============================
        # length = generate_text_length()

        if text_map[category]:
            desc = random.choice(text_map[category])
            # desc = desc[:length]
        else:
            # fallback if category missing
            all_texts = sum(text_map.values(), [])
            # desc = random.choice(all_texts)[:length] if all_texts else "No description available"
            desc = random.choice(all_texts) if all_texts else "No description available"

        tickets.append((
            ticket_id,
            user_id,
            category,
            desc,
            ticket_date
        ))

        user_ticket_counts[user_id] = user_ticket_counts.get(user_id, 0) + 1
        ticket_id += 1

    return pd.DataFrame(tickets, columns=[
        "ticket_id", "user_id", "category",
        "description", "created_at"
    ])