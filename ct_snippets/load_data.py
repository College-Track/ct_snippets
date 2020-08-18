import pandas as pd


def read_csv(location):
    df = pd.read_csv(location)
    return df


def write_data(df):
    df.to_csv("test.csv")

