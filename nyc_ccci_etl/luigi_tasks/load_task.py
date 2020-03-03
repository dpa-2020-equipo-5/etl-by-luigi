import luigi
import json
import os

from nyc_ccci_etl.luigi_tasks.extract_task import ExtractTask
from nyc_ccci_etl.etl.loading_procedure import LoadingProcedure
from nyc_ccci_etl.commons.build_tmp_filename import build_tmp_filename

class LoadTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    
    def requires(self):
        self.tmp_path = build_tmp_filename(str(self.year), str(self.month), str(self.day))
        return ExtractTask(self.year, self.month, self.day, self.tmp_path)

    def run(self):
        etl_load = LoadingProcedure(self.tmp_path)
        result, row_count = etl_load.execute()
        if result:
            print("\n"*3)
            print("-"*100)
            print("Status: OK")
            print("Inserted {} rows".format(str(row_count)))
            print("-"*100)
            print("\n"*3)
        else:
            print("{}Nofify :({}".format("-"*20, "-"*20))
        os.remove(self.tmp_path)
    
    