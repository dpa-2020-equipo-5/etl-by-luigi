import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.etl.clean_procedure import CleanProcedure
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

from nyc_ccci_etl.etl.update_static_centers import UpdateStaticCenters
from .load_clean_inspections_metadata_task import LoadCleanInspectionsMetadataTask
class UpdateCentersTask(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadCleanInspectionsMetadataTask(self.year, self.month, self.day)

    host, database, user, password = get_database_connection_parameters()
    table = "transformed.centers"
    update_centers = UpdateStaticCenters()
    rs, columns = update_centers.execute()

    def rows(self):        
        for element in self.rs:
            yield element