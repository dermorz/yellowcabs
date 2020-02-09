import os
import sys

import environ


DATA_DIR = os.path.join(sys.prefix, "share")


@environ.config(prefix="YC")
class AppConfig:
    base_url = environ.var(default="https://s3.amazonaws.com/nyc-tlc/trip+data/")
    trip_data = environ.var(default="yellow_tripdata")
    local_cache_dir = environ.var(default=DATA_DIR)

    @environ.config()
    class DB:
        url = environ.var(default="sqlite:///results.sqlite")

    db = environ.group(DB)


settings = environ.to_config(AppConfig)
