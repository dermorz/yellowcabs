from datetime import date
from datetime import datetime
import os
from typing import Union

from furl import furl
import requests

from yellowcabs.config import settings


def get_url(date: Union[date, datetime]) -> furl:
    fname = f"{settings.trip_data}_{date.year}-{date.month:02d}.csv"
    return furl(settings.base_url).join(fname)


def get_local_filename(url: furl) -> str:
    return os.path.join(settings.local_cache_dir, url.path.segments[-1])


def download(url: furl) -> str:
    chunk_size = 32 * 1024
    fname = get_local_filename(url)
    if os.path.exists(fname):
        return fname
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(fname, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    return fname
