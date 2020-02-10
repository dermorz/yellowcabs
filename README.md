# yellowcabs

A simple data pipeline to calculate the monthly average trip length and a 45
days rolling average trip length of NY yellow cabs.

*Disclaimer:  I interpreted "trip length" as duration, not as distance*


## Requirements

* Python 3.7
* tox (optional to run tests and linting smoothly)

## Setup

Clone this repository and

```bash
$ pip install .
```

or

```bash
$ pip install dist/yellowcabs-1.0.0-py3-none-any.whl
```

For production use the wheel would probably be pushed to a private pypi/devpi
index and installed from there - or directly copied and installed into a
docker image for production. This depends on how things are being run in
production.

## Configuration

| Environment Variable | Description                   | Default                                         |
| --                   | --                            | --                                              |
| `YC_BASE_URL`        | Base URL of the taxi data     | `"https://s3.amazonaws.com/nyc-tlc/trip+data/"` |
| `YC_TRIP_DATA`       | Kind of data to analyze.      | `"yellow_tripdata"`                             |
| `YC_LOCAL_CACHE_DIR` | Location to store cache data  | `<python environment>/share`                    |
| `YC_DB_URL`          | SQLAlchemy connection\_string | `"sqlite:///results.sqlite"`                    |

## Usage

### CLI

```bash
$ yellowcabs 2019-01
The average trip duration in 01/2019 was 988 seconds.
```

Rounded to full seconds for readability. More exact data is available in the
results from the data pipeline.

### `luigi`

```bash
$ luigi --local-scheduler --module yellowcabs.luigi NYTaxiTripDurationAnalytics --month 2019-01
$ luigi --local-scheduler --module yellowcabs.luigi NYTaxiTripDurationAnalytics --month 2019-02
```

You probably want to run the luigi pipeline on a monthly base by a cronjob.
That way on the start of a month new data-sets from the previous month can be
batch-ingested.

The 45 day rolling average trip duration can be found in the table
`trip_duration_rolling_average`. (database as defined in the config)


## Scaling

If the data ingested gets too big for being held in ram or written to local
temp-files like now, the pipeline would need to be refactored to maybe use
Dask instead of plain pandas and a proper data warehouse or at least a proper
database to store temprorary result sets.

Since data engineering not a big part of my professional experience I
probably went a pretty naive way on my implementation, but I learned something on the way.
