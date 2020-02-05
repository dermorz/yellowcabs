import click

from yellowcabs.helpers import download
from yellowcabs.helpers import get_url
from yellowcabs import processing as p


@click.command()
@click.argument("year-month", type=click.DateTime(formats=["%Y-%m"]))
def average_trip_duration(year_month):
    year = year_month.year
    month = year_month.month
    url = get_url(year_month)
    fname = download(url)
    df = (
        p.load_csv(fname)
        .pipe(p.rename_columns)
        .pipe(p.filter_by_month_year, dt=year_month)
        .pipe(p.calculate_durations)
        .pipe(p.reindex_on_pickup)
        .pipe(p.monthly_average_durations)
    )
    duration = int(round(df["duration"][0]))
    click.echo(
        f"The average trip duration in {month:02d}/{year} was {duration} seconds."
    )
