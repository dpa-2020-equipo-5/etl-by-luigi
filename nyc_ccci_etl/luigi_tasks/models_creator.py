import luigi
import luigi.contrib.postgres
from nyc_ccci_etl.luigi_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.fit_random_forest_and_create_pickle import FitRandomForestAndCreatePickle
from nyc_ccci_etl.luigi_tasks.fit_xgboost_and_create_pickle import FitXGBoostAndCreatePickle
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
class ModelsCreator(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    _complete = False
    def run(self):
        yield FitXGBoostAndCreatePickle(self.year, self.month, self.day), FitRandomForestAndCreatePickle(self.year, self.month, self.day)
        connection = self.output().connect()
        self.output().touch(connection)
        connection.commit()
        connection.close()

    def output(self):
        update_id = "{}{}{}".format(str(self.year), str(self.month), str(self.day))
        
        host, database, user, password = get_database_connection_parameters()
        return luigi.contrib.postgres.PostgresTarget(host, database, user, password, 'table_updates', update_id)   

    