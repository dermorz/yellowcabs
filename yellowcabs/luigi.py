import os

import luigi

from yellowcabs import helpers as h
from yellowcabs import processing as p
from yellowcabs.config import settings


class GetNYTaxiMontlyData(luigi.Task):
    month = luigi.MonthParameter()

    def run(self):
        url = h.get_url(self.month)
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
            os.path.join(settings.local_cache_dir, f"taxi_data_clean_{self.month}.pickle")
        )

    def run(self):
        df = (
            p.load_csv(self.input().path)
            .pipe(p.rename_columns)
            .pipe(p.filter_by_month_year, self.month)
        )
        df.to_pickle(self.output().path)
