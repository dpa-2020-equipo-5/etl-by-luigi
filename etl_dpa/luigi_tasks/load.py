import luigi
import json
import os

from etl_dpa.luigi_tasks.extract import ExtractTask
from etl_dpa.etl.loading_procedure import LoadingProcedure

class LoadTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    tmp_path = luigi.Parameter()
    def requires(self):
        return ExtractTask(self.year, self.month, self.day)

    def run(self):
        etl_load = LoadingProcedure(str(self.tmp_path))
        result = etl_load.execute()
        print(result)
    
    def complete(self):
        os.remove('tmp/' + str(self.tmp_path))