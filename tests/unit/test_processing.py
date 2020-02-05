from datetime import datetime
import os

import numpy as np
import pandas as pd
import pandas.api.types as ptypes
import pytest

from yellowcabs.config import settings
from yellowcabs.processing import calculate_durations
from yellowcabs.processing import daily_average_durations
from yellowcabs.processing import filter_by_month_year
from yellowcabs.processing import load_csv
from yellowcabs.processing import monthly_average_durations
from yellowcabs.processing import n_day_rolling_average_duration
from yellowcabs.processing import rename_columns


def test_load_csv():
    df = load_csv(os.path.join(settings.local_cache_dir, "yellow_tripdata_2019-02.csv"))
    assert ptypes.is_datetime64_dtype(df["tpep_pickup_datetime"])
    assert ptypes.is_datetime64_dtype(df["tpep_dropoff_datetime"])
    assert all(df.columns == ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    assert len(df) == 10


def test_rename_columns():
    df = pd.DataFrame(columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    renamed = rename_columns(df)
    assert all(renamed.columns == ["pickup", "dropoff"])


@pytest.fixture
def sample_data():
    return pd.DataFrame(
        data={
            "pickup": [
                datetime(2020, 1, 1, 12, 20),
                datetime(2020, 2, 1, 12, 20),
                datetime(2019, 1, 1, 12, 20),
            ],
            "dropoff": [
                datetime(2020, 1, 1, 12, 30),
                datetime(2020, 2, 1, 12, 31),
                datetime(2019, 1, 1, 12, 32),
            ],
        },
        columns=["pickup", "dropoff"],
    )


def test_filter_by_month_year(sample_data):
    assert len(filter_by_month_year(sample_data, datetime(2020, 1, 1))) == 1
    assert len(filter_by_month_year(sample_data, datetime(2019, 1, 1))) == 1
    assert len(filter_by_month_year(sample_data, datetime(2020, 3, 1))) == 0


def test_calculate_durations(sample_data):
    df = calculate_durations(sample_data)
    assert all(df["duration"] == [10 * 60, 11 * 60, 12 * 60])


def test_daily_average_durations():
    df = pd.DataFrame(
        data={"duration": [1, 3, 2, 4, 3, 5]},
        columns=["duration"],
        index=pd.date_range(start="2020-01-01", freq="12H", periods=6),
    )
    assert all(daily_average_durations(df)["duration"] == [2, 3, 4])


def test_monthly_average_durations():
    df = pd.DataFrame(
        data={"duration": [1] * 30 + [3] * 30},
        columns=["duration"],
        index=pd.date_range(start="2020-04-01", freq="12H", periods=60),
    )
    assert all(monthly_average_durations(df)["duration"] == [2])


def test_rolling_average_duration():
    """Rolling average of n days only possible from day n on"""
    df = pd.DataFrame(
        data={"duration": [1] * 30},
        columns=["duration"],
        index=pd.date_range(start="2020-04-01", freq="1D", periods=30),
    )
    rolling_10 = pd.DataFrame(
        data={"duration": [np.nan] * 9 + [1] * 21},
        columns=["duration"],
        index=pd.date_range(start="2020-04-01", freq="1D", periods=30),
    )
    pd.testing.assert_frame_equal(rolling_10, n_day_rolling_average_duration(df, 10))
