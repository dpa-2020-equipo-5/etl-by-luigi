import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sklearn as sk
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.metrics import mean_squared_error
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class NYCCCCIRandomForest:
    def execute(self):
        host, database, user, password = get_database_connection_parameters()
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        engine = create_engine(engine_string)
        tabla_3 = pd.read_sql_table('centers', engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', engine, schema="transformed")


        return "MODELO BONITO YA AJUSTADO"
        '''
        rforest = RandomForestClassifier(n_estimators=600, class_weight="balanced", max_depth=8, criterion='gini')
        rforest.fit(X_train,Y_train.values.ravel())
        return rforst
        '''