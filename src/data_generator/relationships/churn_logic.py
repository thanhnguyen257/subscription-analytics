# relationships/churn_logic.py

import numpy as np
import pandas as pd


def identify_risky_users(tickets_df: pd.DataFrame) -> set:
    """
    Users with negative signals → higher churn probability
    """
    negative_categories = ["billing", "cancellation"]

    risky_users = tickets_df[
        tickets_df["category"].isin(negative_categories)
    ]["user_id"].unique()

    return set(risky_users)


def identify_payment_issues(payments_df: pd.DataFrame) -> set:
    """
    Users with failed/refunded payments
    """
    bad_status = ["failed", "refunded"]

    problematic_subs = payments_df[
        payments_df["payment_status"].isin(bad_status)
    ]["subscription_id"].unique()

    return set(problematic_subs)


def apply_churn_bias(
    subs_df: pd.DataFrame,
    risky_users: set,
    problematic_subs: set
) -> pd.DataFrame:
    """
    Adjust subscription status based on behavior
    """

    df = subs_df.copy()

    # user-level churn bias
    user_mask = df["user_id"].isin(risky_users)

    df.loc[user_mask, "status"] = np.random.choice(
        ["cancelled", "expired"],
        size=user_mask.sum(),
        p=[0.7, 0.3]
    )

    # payment issue bias
    sub_mask = df["subscription_id"].isin(problematic_subs)

    df.loc[sub_mask, "status"] = np.random.choice(
        ["cancelled", "expired"],
        size=sub_mask.sum(),
        p=[0.6, 0.4]
    )

    return df


def enforce_ticket_churn_link(
    tickets_df: pd.DataFrame,
    subs_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Ensure cancellation tickets correlate with cancelled subscriptions
    """

    cancelled_subs = subs_df[
        subs_df["status"] == "cancelled"
    ][["user_id"]]

    cancelled_users = set(cancelled_subs["user_id"].unique())

    mask = tickets_df["category"] == "cancellation"

    tickets_df.loc[mask, "user_id"] = np.random.choice(
        list(cancelled_users),
        size=mask.sum()
    )

    return tickets_df