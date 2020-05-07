import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.etl.clean_procedure import CleanProcedure
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

from nyc_ccci_etl.etl.inspections_transformer import InspectionsTransformer
from .update_centers_task import UpdateCentersTask

class LoadTransformedInspectionsTask(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    
    host, database, user, password = get_database_connection_parameters()
    table = "transformed.inspections"

    def requires(self):
        return UpdateCentersTask(self.year, self.month, self.day)
    
    def run(self):
        transform_inspections = InspectionsTransformer()
        self._rows, self.columns = transform_inspections.execute()
        super().run()

    def rows(self):        
        for element in self._rows:
            yield element
