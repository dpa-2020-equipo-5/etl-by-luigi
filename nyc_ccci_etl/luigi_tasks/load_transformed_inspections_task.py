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

    def requires(self):
        return UpdateCentersTask(self.year, self.month, self.day)

    host, database, user, password = get_database_connection_parameters()
    table = "transformed.inspections"
    transform_inspections = InspectionsTransformer()
    rs, columns = transform_inspections.execute()

    def rows(self):        
        for element in self.rs:
            yield element