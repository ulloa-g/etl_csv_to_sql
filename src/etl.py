import pandas as pd


def extract(csv_path):
    return pd.read_csv(csv_path)


if __name__ == "__main__":
    csv_path = "../data/input/bank_marketing.csv"
    df = extract(csv_path)
