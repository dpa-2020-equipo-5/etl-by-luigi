import json
import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.orchestrator_tasks.fit_random_forest_and_create_pickle import FitRandomForestAndCreatePickle
from nyc_ccci_etl.orchestrator_tasks.load_aequitas_groups import LoadAequitasGroups
from nyc_ccci_etl.orchestrator_tasks.load_aequitas_bias import LoadAequitasBias
from nyc_ccci_etl.orchestrator_tasks.load_aequitas_fairness import LoadAequitasFairness

class LoadBiasFairnessMetadata(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    _complete = False
    
    def requires(self):
        return (
            LoadAequitasGroups(self.year, self.month, self.day),
            # LoadAequitasBias(self.year, self.month, self.day),
            # LoadAequitasFairness(self.year, self.month, self.day)
        )

    def run(self):
        self._complete = True
    

    def complete(self):
        return self._complete