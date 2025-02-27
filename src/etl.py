import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def extract(csv_path):
    return pd.read_csv(csv_path)


def transform(df):
    df["job"] = df["job"].str.replace(".", "_")
    df["education"] = df["education"].str.replace(".", "_")
    df["education"] = df["education"].replace("unknown", np.nan, inplace=True)
    df["credit_default"] = df["credit_default"].map({"yes": 1}).fillna(0)
    df["credit_default"] = df["credit_default"].astype(bool)
    df["mortgage"] = df["mortgage"].map({"yes": 1}).fillna(0)
    df["mortgage"] = df["mortgage"].astype(bool)
    df["previous_outcome"] = df["previous_outcome"].map({"success": 1}).fillna(0)
    df["previous_outcome"] = df["previous_outcome"].astype(bool)
    df["campaign_outcome"] = df["campaign_outcome"].map({"yes": 1}).fillna(0)
    df["campaign_outcome"] = df["campaign_outcome"].astype(bool)
    df["year"] = 2022
    months = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    }
    df["last_contact_date"] = df["year"].astype(str) + "-" + df["month"].map(months) + "-" + df["day"].astype(str).str.zfill(2)
    df["last_contact_date"] = pd.to_datetime(df["last_contact_date"])
    df.drop("year", axis=1, inplace=True)
    return df


def load_to_csv(df_clean):
    client = df_clean[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    campaign = df_clean[
        [
            "client_id", "number_contacts", "last_contact_date", "contact_duration",
            "previous_campaign_contacts", "previous_outcome", "campaign_outcome"
        ]
    ]
    economics = df_clean[["client_id", "cons_price_idx", "euribor_three_months"]]
    client.to_csv("../data/output/client.csv", index=False)
    campaign.to_csv("../data/output/campaign.csv", index=False)
    economics.to_csv("../data/output/economics.csv", index=False)


def load_to_sql(df_clean, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    df_clean.to_sql("bank", engine, index=False, if_exists="replace")


if __name__ == "__main__":
    csv_path = "../data/input/bank_marketing.csv"
    db_path = "../data/output/bank_marketing.db"
    df = extract(csv_path)
    df_clean = transform(df)
    load_to_csv(df_clean)
