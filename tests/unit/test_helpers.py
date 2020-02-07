"""Unittests are assuming default settings"""

from datetime import date
from datetime import datetime
import os

from furl import furl

from yellowcabs.config import settings
from yellowcabs.helpers import get_local_filename
from yellowcabs.helpers import get_url


def test_get_url():
    assert get_url(datetime(2020, 5, 1)) == furl(
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-05.csv"
    )
    assert get_url(date(2020, 5, 1)) == furl(
        "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-05.csv"
    )


def test_get_local_filename():
    url = furl("https://example.com/foo/robots.txt")
    assert get_local_filename(url) == os.path.join(
        settings.local_cache_dir, "robots.txt"
    )
