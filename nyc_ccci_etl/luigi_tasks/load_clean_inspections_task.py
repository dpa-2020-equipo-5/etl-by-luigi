import luigi
from luigi.contrib.postgres import CopyToTable
from nyc_ccci_etl.luigi_tasks.load_raw_inspections_metadata_task import LoadRawInspectionsMetadataTask
from nyc_ccci_etl.etl.extraction_procedure import ExtractionProcedure
from nyc_ccci_etl.etl.clean_procedure import CleanProcedure
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class LoadCleanInspectionsTask(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    
    def requires(self):
        return LoadRawInspectionsMetadataTask(self.year, self.month, self.day)

    host, database, user, password = get_database_connection_parameters()
    table = "clean.inspections"
    
    etl_extraction = ExtractionProcedure(2020,3,15)
    inspections_json_data = etl_extraction.execute()
    clean = CleanProcedure(inspections_json_data)
    _rows, _columns = clean.execute()
    columns = [ (c, 'VARCHAR') for c in _columns]

    def rows(self):        
        for element in self._rows:
            yield element
        
        with open('tmp/inserted_vars_clean', 'w') as f:
            if len(self._rows) > 0:
                f.write(",".join(self._columns))
            else:
                f.write("")
                
        with open('tmp/inserted_records_clean', 'w') as f:
            f.write(str(len(self._rows)))