import luigi
import json
import os

from etl_dpa.etl.extraction_procedure import ExtractionProcedure

class ExtractTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def build_tmp_filename(self):
        return "inspections_{}_{}_{}".format(str(self.year), str(self.month), str(self.day))

    def run(self):
        etl_extract = ExtractionProcedure(self.year, self.month, self.day)
        result = etl_extract.execute()

        with self.output().open('w') as output_file:
            json.dump(result, output_file)

    def output(self):
        return luigi.local_target.LocalTarget("tmp/{}.json".format(self.build_tmp_filename()))
