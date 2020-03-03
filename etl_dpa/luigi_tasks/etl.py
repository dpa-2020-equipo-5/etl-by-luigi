import luigi
import json
import os

from etl_dpa.luigi_tasks.extract import ExtractTask
from etl_dpa.luigi_tasks.load import LoadTask


from etl_dpa.commons.build_tmp_filename import build_tmp_filename

class ETLTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadTask(self.year, self.month, self.day)

    def run(self):
        with self.output().open('w') as output_file:
            output_file.write("done.")

    def output(self):
        tmp_name = build_tmp_filename(str(self.year), str(self.month), str(self.day))
        return luigi.local_target.LocalTarget("tmp/{}.json".format(tmp_name))