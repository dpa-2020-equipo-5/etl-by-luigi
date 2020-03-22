from datetime import datetime
import json
import luigi
import luigi.contrib.postgres

from nyc_ccci_etl.etl.extraction_procedure import ExtractionProcedure
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from nyc_ccci_etl.commons.configuration import get_database_name
from nyc_ccci_etl.commons.configuration import get_database_table
from nyc_ccci_etl.commons.configuration import get_database_user
from nyc_ccci_etl.utils.get_os_user import get_os_user
from nyc_ccci_etl.utils.get_current_ip import get_current_ip

class ETLTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    

    def insert_inspections(self, conn, inspections):
        with conn.cursor() as cur:
            cur.executemany(
                'INSERT INTO raw.inspections(inspection) VALUES(%s)', 
                [(json.dumps(d).replace("'", "''"),) for d in inspections]
            )
    def insert_metadata(self, conn, inspections):
        params_string = "year={} month={} day={}".format(str(self.year), str(self.month), str(self.day))
        if len(inspections) > 0:
            inserted_vars = ",".join(inspections[0].keys())
        else:
            inserted_vars =  ""

        metadata = "'{}','{}',{},'{}','{}','{}','{}','{}','{}','{}','{}'".format(
            str(datetime.now(tz=None)),
            params_string,
            str(len(inspections)),
            get_os_user(),
            get_current_ip(),
            get_database_name(),
            "raw",
            get_database_table(),
            get_database_user(),
            inserted_vars,
            "etl"
        )
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO raw.etl_metadata(executed_at, task_params, record_count, execution_user, source_ip, database_name, database_schema, database_table, database_user, vars, script_tag) VALUES (%s)'%metadata
            )
        

    def run(self):
        #Creamos un objeto de la clase ExtracionProcedure con los parámetros de fecha
        etl_extraction = ExtractionProcedure(self.year, self.month, self.day)

        #Ejecutamos la extracción y se nos regresa una lista de diccionarios (json)
        inspections_json_data = etl_extraction.execute()
        
        #Creamos conexión con postgres a través de nuestro PostgresTarget
        connection = self.output().connect()
        
        #ejectuamos INSERTs de los objetos json en la tabla `inspections` 
        self.insert_inspections(connection, inspections_json_data)

        #ejecutamos INSERTs de metadata
        self.insert_metadata(connection, inspections_json_data)
        
        #actualizar la `marker_table`: ésta es la manera en la que luigi sabrá que cierto TASK YA SE EJECUTÓ 
        self.output().touch(connection)

        #commit y cierre
        connection.commit()
        connection.close()

    #Output de PostgresTarget
    def output(self):
        #Este update_id se usará para identificar el TASK que se acaba de ejecutar para que luigi no repita Tasks
        update_id = "{}{}{}".format(str(self.year), str(self.month), str(self.day))
        
        host, database, user, password, table = get_database_connection_parameters()
        return luigi.contrib.postgres.PostgresTarget(host, database, user, password, table, update_id)    