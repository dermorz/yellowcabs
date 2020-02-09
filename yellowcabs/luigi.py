import os

from dateutil.parser import parse
import luigi
from luigi.contrib import sqla
import pandas as pd
import sqlalchemy as sa

from yellowcabs.config import settings
from yellowcabs import helpers as h
from yellowcabs import processing as p


class GetNYTaxiMontlyData(luigi.Task):
    month = luigi.MonthParameter()

    def run(self):
        url = h.get_url(self.month)
        self.output().makedirs()
        fname = h.download(url)
        self.output().fs.move(fname, self.output().path)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(settings.local_cache_dir, f"taxi_data_{self.month}.csv")
        )


class CleanUpTaxiData(luigi.Task):
    month = luigi.MonthParameter()

    def requires(self):
        return GetNYTaxiMontlyData(month=self.month)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                settings.local_cache_dir, f"taxi_data_clean_{self.month}.pickle"
            )
        )

    def run(self):
        df = (
            p.load_csv(self.input().path)
            .pipe(p.rename_columns)
            .pipe(p.filter_by_month_year, self.month)
        )
        df.to_pickle(self.output().path)


class CalculateTripDurations(luigi.Task):
    month = luigi.MonthParameter()

    def requires(self):
        return CleanUpTaxiData(month=self.month)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                settings.local_cache_dir, f"trip_durations_{self.month}.pickle"
            )
        )

    def run(self):
        df = pd.read_pickle(self.input().path)
        trip_durations = p.calculate_durations(df).pipe(p.reindex_on_pickup)
        trip_durations.to_pickle(self.output().path)


class CalculateDailyAverageTripDuration(luigi.Task):
    month = luigi.MonthParameter()

    def requires(self):
        return CalculateTripDurations(month=self.month)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                settings.local_cache_dir,
                f"trip_duration_daily_average_{self.month}.csv",
            )
        )

    def run(self):
        df = pd.read_pickle(self.input().path)
        daily_averages = p.daily_average_durations(df)
        daily_averages.to_csv(self.output().path, header=False)


class WriteDailyAveragesToDB(sqla.CopyToTable):
    month = luigi.MonthParameter()

    def requires(self):
        return CalculateDailyAverageTripDuration(month=self.month)

    columns = [(["date", sa.DATE], {"primary_key": True}), (["duration", sa.Float], {})]
    connection_string = settings.db.url
    table = "trip_duration_daily_average"
    column_separator = ","

    def rows(self):
        for date_str, duration in super().rows():
            yield parse(date_str).date(), duration


class CalculateMonthlyAverageTripDuration(luigi.Task):
    month = luigi.MonthParameter()

    def requires(self):
        return CalculateTripDurations(month=self.month)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                settings.local_cache_dir,
                f"trip_duration_monthly_average_{self.month}.csv",
            )
        )

    def run(self):
        df = pd.read_pickle(self.input().path)
        daily_averages = p.monthly_average_durations(df)
        daily_averages.to_csv(self.output().path, header=False)


class WriteMonthlyAveragesToDB(sqla.CopyToTable):
    month = luigi.MonthParameter()

    def requires(self):
        return CalculateMonthlyAverageTripDuration(month=self.month)

    columns = [(["date", sa.DATE], {"primary_key": True}), (["duration", sa.Float], {})]
    connection_string = settings.db.url
    table = "trip_duration_monthly_average"
    column_separator = ","

    def rows(self):
        for date_str, duration in super().rows():
            # Month represented by the first day of each month instead of the last one
            date = parse(date_str).date().replace(day=1)
            yield date, duration


class CalculateTripDurationRollingAverage45Days(luigi.Task):
    month = luigi.MonthParameter()

    def requires(self):
        return WriteDailyAveragesToDB(month=self.month)

    def run(self):
        df = pd.read_sql_table(
            "trip_duration_daily_average",
            con=self.input().engine,
            parse_dates=["date"],
            index_col="date",
        )
        rolling_averages = p.n_day_rolling_average_duration(df, days=45)
        rolling_averages.to_csv(self.output().path, header=False)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                settings.local_cache_dir,
                f"rolling_average_trip_durations_{self.month}.csv",
            )
        )


class WriteRollingAveragesToDB(sqla.CopyToTable):
    month = luigi.MonthParameter()

    def requires(self):
        return CalculateTripDurationRollingAverage45Days(month=self.month)

    columns = [
        (["date", sa.DATE], {"primary_key": True}),
        (["rolling_average_45d", sa.Float], {"nullable": True}),
    ]
    connection_string = settings.db.url
    table = "trip_duration_rolling_average"
    column_separator = ","

    def rows(self):
        for date_str, duration in super().rows():
            date = parse(date_str).date()
            if date.month != self.month.month:
                continue
            if not duration:
                duration = None
            yield date, duration


class NYTaxiTripDurationAnalytics(luigi.WrapperTask):
    month = luigi.MonthParameter()

    def requires(self):
        yield WriteDailyAveragesToDB(self.month)
        yield WriteMonthlyAveragesToDB(self.month)
        yield WriteRollingAveragesToDB(self.month)
