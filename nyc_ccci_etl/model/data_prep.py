import pandas as pd
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
class DataPrep:
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

    def train_test_split(self):
        tabla_3 = pd.read_sql_table('centers', self.engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', self.engine, schema="transformed")

        tabla_3 = tabla_3.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status', 'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual', 'violationavgratepercent', 
                        'averagetotaleducationalworkers', 'averagepublichealthhazardiolationrate'], axis=1)
        tabla_3.rename(columns={'dc_id':'center_id'}, inplace=True)
        tabla_3 = tabla_3.reset_index(drop=True)
        tabla_5 = pd.merge(tabla_4, tabla_3)

        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype('int')
        tabla_5['violationratepercent'] = tabla_5['violationratepercent'].astype('float')
        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype('int')
        tabla_5['publichealthhazardviolationrate'] = tabla_5['publichealthhazardviolationrate'].astype('float')
        tabla_5['criticalviolationrate'] = tabla_5['criticalviolationrate'].astype('float')
        tabla_5['avgcriticalviolationrate'] = tabla_5['avgcriticalviolationrate'].astype('float')
        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate'], axis=1)
        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5:
            tabla_5[col] = tabla_5[col].astype(float)

        # tabla_5['violationcategory_public_health_hazard'] = tabla_5['violationcategory_public_health_hazard'].astype(int)
        # tabla_5['inspection_year'] = tabla_5['inspection_year'].astype(int)

        count_class_0, count_class_1 = tabla_5.violationcategory_public_health_hazard.value_counts()
        df_class_0 = tabla_5[tabla_5['violationcategory_public_health_hazard'] == 0]
        df_class_1 = tabla_5[tabla_5['violationcategory_public_health_hazard'] == 1]
        df_class_0_over = df_class_0.sample(count_class_1, replace=True)
        df_test_over = pd.concat([df_class_1, df_class_0_over], axis=0)

        df_train = df_test_over.loc[df_test_over['inspection_year'] != 2020.0]
        df_test = df_test_over.loc[df_test_over['inspection_year'] == 2020.0]

        Y_train = df_train[['violationcategory_public_health_hazard']]
        Y_test = df_test[['violationcategory_public_health_hazard']]
        X_train = df_train[[i for i in df_train.keys() if i not in Y_train]]
        X_test = df_test[[i for i in df_test.keys() if i not in Y_test]]

        return X_train, Y_train, X_test, Y_test

