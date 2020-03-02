import luigi
import json
import os

from etl_dpa.tasks.extract import ExtractTask

class LoadTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return ExtractTask(self.year, self.month, self.day)

    def run(self):
        print("Soy un pez")