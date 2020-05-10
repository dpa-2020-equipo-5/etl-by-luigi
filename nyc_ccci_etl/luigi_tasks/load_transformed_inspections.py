import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.luigi_tasks.load_clean_inspections_metadata import LoadCleanInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.load_raw_inspections_metadata import LoadRawInspectionsMetadata

from nyc_ccci_etl.etl.inspections_transformer import InspectionsTransformer
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters



class LoadTransformedInspections(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    
    host, database, user, password = get_database_connection_parameters()
    table = "transformed.inspections"

    def requires(self):
        return (
            LoadCleanInspectionsMetadata(self.year, self.month, self.day), LoadRawInspectionsMetadata(self.year, self.month, self.day)
        )
    
    def run(self):
        transform_inspections = InspectionsTransformer()
        self._rows, self.columns = transform_inspections.execute()

        super().run()

    def rows(self):        
        for element in self._rows:
            yield element

        with open('tmp/inserted_vars_transformed', 'w') as f:
            if len(self.columns) > 0:
                f.write(str(self.columns))
            else:
                f.write("")
                
        with open('tmp/inserted_records_transformed', 'w') as f:
            f.write(str(len(self._rows)))