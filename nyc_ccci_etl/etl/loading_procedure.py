import psycopg2
import json

from nyc_ccci_etl.commons.configuration import get_database_connection_url
from nyc_ccci_etl.utils.json_to_sql_inserts import json_to_sql_inserts

class LoadingProcedure():
    def __init__(self, json_path):
        self.tmp_json_path = json_path
        self.connection = psycopg2.connect(get_database_connection_url())
        self.cursor = self.connection.cursor()

    def execute(self):
        try:
            with open(self.tmp_json_path) as json_data:
                record_list = json.load(json_data)
            if len(record_list) == 0:
                return True
            
            sql = json_to_sql_inserts('rawdata.inspections',record_list)
            
            self.cursor.execute(sql)
            self.connection.commit()
            self.cursor.close()
            self.connection.close()
            return True
        except:
            return False