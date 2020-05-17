import luigi
from luigi.contrib.postgres import CopyToTable
from nyc_ccci_etl.luigi_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.load_update_centers_metadata import LoadUpdateCentersMetadata
from nyc_ccci_etl.model.random_forest_grid_search import RandomForestGridSearch
from nyc_ccci_etl.model.data_preparator import DataPreparator
from nyc_ccci_etl.luigi_tasks.fit_random_forest_and_create_pickle import FitRandomForestAndCreatePickle

class ModelGridSearch(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    _complete = False
    

    def run(self):
        model_params = RandomForestGridSearch.find_best_params()
        self._complete = True

    def complete(self):
        return self._complete