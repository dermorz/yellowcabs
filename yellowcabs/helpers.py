from datetime import datetime
import os

from furl import furl
import requests
from tqdm import tqdm

from yellowcabs.config import settings


def get_url(date: datetime) -> furl:
    fname = f"{settings.trip_data}_{date.year}-{date.month:02d}.csv"
    return furl(settings.base_url).join(fname)


def get_local_filename(url: furl) -> str:
    return os.path.join(settings.local_cache_dir, url.path.segments[-1])


def download(url: furl) -> str:
    chunk_size = 1024
    fname = get_local_filename(url)
    if os.path.exists(fname):
        return fname
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(fname, "wb") as f:
            tq = tqdm(
                response.iter_content(chunk_size=chunk_size),
                total=int(response.headers["Content-length"]),
                unit="bytes",
                unit_scale=True,
                leave=False,
            )
            for chunk in tq:
                if chunk:
                    f.write(chunk)
                    tq.update(chunk_size)
    return fname
