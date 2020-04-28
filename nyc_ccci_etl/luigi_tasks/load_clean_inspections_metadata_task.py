import json
import luigi
from luigi.contrib.postgres import CopyToTable
from datetime import datetime
from nyc_ccci_etl.etl.extraction_procedure import ExtractionProcedure
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.utils.get_os_user import get_os_user
from nyc_ccci_etl.utils.get_current_ip import get_current_ip
from random import randint

from .load_clean_inspections_task import LoadCleanInspectionsTask
class LoadCleanInspectionsMetadataTask(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    def requires(self):
        return  LoadCleanInspectionsTask(self.year, self.month, self.day)
    

    inserted_vars = ""
    with open("tmp/inserted_vars_clean") as f:
        inserted_vars = f.read().strip()
    
    inserted_records = ""
    with open("tmp/inserted_records_clean") as f:
        inserted_records = f.read().strip()

    host, database, user, password = get_database_connection_parameters()
    table = "clean.metadata"
    schema = "clean"

    columns = [ 
        ("executed_at", "timestamp"),
        ("task_params", "varchar"),
        ("record_count", "integer"),
        ("execution_user", "varchar"),
        ("source_ip", "varchar"),
        ("database_name", "varchar"),
        ("database_schema", "varchar"),
        ("database_table", "varchar"),
        ("database_user", "varchar"),
        ("vars", "varchar"),
        ("script_tag", "varchar")
    ]

    def rows(self):
        params_string = "year={} month={} day={}".format(str(self.year), str(self.month), str(self.day))
        row = (
            str(datetime.now(tz=None)),
            params_string,
            self.inserted_records,
            get_os_user(),
            get_current_ip(),
            self.database,
            self.schema,
            self.table,
            self.user,
            self.inserted_vars,
            "etl"
        )
        yield row