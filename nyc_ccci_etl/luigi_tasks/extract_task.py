import luigi
import json
import os

from nyc_ccci_etl.etl.extraction_procedure import ExtractionProcedure

class ExtractTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    tmp_path = luigi.Parameter()

    def run(self):
        etl_extract = ExtractionProcedure(self.year, self.month, self.day)
        result = etl_extract.execute()

        with self.output().open('w') as output_file:
            json.dump(result, output_file)

    def output(self):
        return luigi.local_target.LocalTarget(self.tmp_path)
