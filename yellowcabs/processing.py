from datetime import date
from datetime import datetime
from typing import Union

import pandas as pd


def load_csv(fname: str) -> pd.DataFrame:
    return pd.read_csv(
        fname,
        usecols=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    )


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={"tpep_pickup_datetime": "pickup", "tpep_dropoff_datetime": "dropoff"}
    )


def filter_by_month_year(df: pd.DataFrame, dt: Union[date, datetime]) -> pd.DataFrame:
    """Filter by rides that started in month and year in given date or datetime"""
    return df[(df["pickup"].dt.month == dt.month) & (df["pickup"].dt.year == dt.year)]


def calculate_durations(df: pd.DataFrame) -> pd.DataFrame:
    """Durations in seconds"""
    df = df.copy()
    df["duration"] = (df["dropoff"] - df["pickup"]).dt.seconds
    return df


def reindex_on_pickup(df: pd.DataFrame) -> pd.DataFrame:
    return df.set_index("pickup")


def daily_average_durations(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("D").mean()


def monthly_average_durations(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("M").mean()


def n_day_rolling_average_duration(df: pd.DataFrame, days: int) -> pd.DataFrame:
    return df.rolling(days).mean()
