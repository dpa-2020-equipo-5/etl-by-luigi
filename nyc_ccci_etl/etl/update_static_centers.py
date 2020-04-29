import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
#from nyc_ccci_etl.etl.feature_engineering import FeatureEngineering
#fe = FeatureEngineering()
class UpdateStaticCenters:
    def __init__(self):
        host, database, user, password = get_database_connection_parameters()
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)
        self.rows = None
        self.columns = None
        self.column_tupples =  None


    def execute(self):
        df = pd.read_sql_table('inspections', self.engine, schema="clean")
        #Seleccionando las variables estaticas de la tabla limpia
        tabla_3 = df.iloc[:, 0:28]

        #Tirando los duplicados para que queden los centros únicos y sus características
        tabla_3 = tabla_3.drop_duplicates()

        #Creando dummies para borough, program_type, facility_type
        dummies = ["borough","programtype", "facilitytype"]
        df_1 = pd.get_dummies(tabla_3[dummies])
        tabla_3 = tabla_3.join(df_1)

        #Seleccionando las varibales a usar en el modelo
        tabla_3 = tabla_3.drop(df.columns[[0,1,2,3,4,5,6,7,8,9,10,15,16,17,18,19,21,23,25,27]], axis=1)
        return [tuple(x) for x in tabla_3.to_numpy()], [(c, 'VARCHAR') for c in list(tabla_3.columns)]

    def get_rows(self):
        return self.rows
    def get_columns(self):
        return self.column_tupples

