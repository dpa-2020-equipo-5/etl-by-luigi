import luigi
import luigi.contrib.s3
from nyc_ccci_etl.luigi_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.fit_random_forest_and_create_pickle import FitRandomForestAndCreatePickle
from nyc_ccci_etl.luigi_tasks.fit_xgboost_and_create_pickle import FitXGBoostAndCreatePickle
import pickle
class ModelsCreator(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    _complete = False
    def run(self):
        yield FitXGBoostAndCreatePickle(self.year, self.month, self.day), FitRandomForestAndCreatePickle(self.year, self.month, self.day)
        self._complete = True

    def complete(self):
        return self._complete

    