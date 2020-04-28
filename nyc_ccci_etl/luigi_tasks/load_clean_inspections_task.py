import luigi
from luigi.contrib.postgres import CopyToTable

from nyc_ccci_etl.etl.extraction_procedure import ExtractionProcedure
from nyc_ccci_etl.etl.clean_procedure import CleanProcedure
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from .raw_stage import RawStage
class LoadCleanInspectionsTask(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return RawStage(self.year, self.month, self.day)

    host, database, user, password = get_database_connection_parameters()
    table = "clean.inspections"

    #columnas de clean.inspections
    columns = [
        ('centername', 'VARCHAR'),
        ('legalname', 'VARCHAR'),
        ('building', 'VARCHAR'),
        ('street', 'VARCHAR'),
        ('borough', 'VARCHAR'),
        ('zipcode', 'VARCHAR'),
        ('phone', 'VARCHAR'),
        ('permitnumber', 'VARCHAR'),
        ('permitexp', 'VARCHAR'),
        ('status', 'VARCHAR'),
        ('agerange', 'VARCHAR'),
        ('maximumcapacity', 'VARCHAR'),
        ('dc_id', 'VARCHAR'),
        ('programtype', 'VARCHAR'),
        ('facilitytype', 'VARCHAR'),
        ('childcaretype', 'VARCHAR'),
        ('bin', 'VARCHAR'),
        ('url', 'VARCHAR'),
        ('datepermitted', 'VARCHAR'),
        ('actual', 'VARCHAR'),
        ('violationratepercent', 'VARCHAR'),
        ('violationavgratepercent', 'VARCHAR'),
        ('totaleducationalworkers', 'VARCHAR'),
        ('averagetotaleducationalworkers', 'VARCHAR'),
        ('publichealthhazardviolationrate', 'VARCHAR'),
        ('averagepublichealthhazardiolationrate', 'VARCHAR'),
        ('criticalviolationrate', 'VARCHAR'),
        ('avgcriticalviolationrate', 'VARCHAR'),
        ('inspectiondate', 'VARCHAR'),
        ('regulationsummary', 'VARCHAR'),
        ('violationcategory', 'VARCHAR'),
        ('healthcodesubsection', 'VARCHAR'),
        ('violationstatus', 'VARCHAR'),
        ('inspectionsummaryresult', 'VARCHAR')
    ]
    def rows(self):        
        etl_extraction = ExtractionProcedure(self.year,self.month,self.day)

        #Ejecutamos la extracción y se nos regresa una lista de diccionarios (json)
        inspections_json_data = etl_extraction.execute()
        
        #Ejecutamos los scripts de limpieza
        clean = CleanProcedure(inspections_json_data)
        rows = clean.execute()
        for element in rows:
            yield element