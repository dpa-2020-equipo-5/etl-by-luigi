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
        if etl_load.execute():
            print("{}Alert admins that all ok{}".format("-"*20, "-"*20))
        else:
            print("{}Nofify :({}".format("-"*20, "-"*20))
        os.remove(self.tmp_path)
    
    