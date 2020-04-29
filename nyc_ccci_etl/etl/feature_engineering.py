import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
import json
class FeatureEngineering:
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

    def execute(self):
        df = pd.read_sql_table('inspections', self.engine, schema="clean")
        df.dropna(inplace=True)
        df['inspectionsummaryresult'] = df['inspectionsummaryresult'].astype('str')
        df_3 = pd.DataFrame(df.inspectionsummaryresult.str.split('_-_',1).tolist(), columns= ['reason', 'result'])
        df_3['result'] = df_3['result'].astype('str')
        df_4 = pd.DataFrame(df_3.result.str.split(';_',1).tolist(), columns = ['result_1', 'result_2'])
        df_3 = df_3.drop(df_3.columns[[1]], axis=1)
        df = pd.concat([df, df_3, df_4], axis=1)
        df = df.drop(['inspectionsummaryresult'], axis = 1)
        df.reason.value_counts(dropna=False)
        df['initialannualinspection'] = df.reason.apply(lambda x: 1 if x == "initialannualinspection" else 0)
        df.initialannualinspection.value_counts(dropna=False)
        df = df.drop(['reason'], axis=1) #Eliminamos la variable reason
        dummies = ["result_1", "result_2"]
        df_2 = df[[i for i in df.keys() if i not in dummies]]
        df_3 = pd.get_dummies(df[dummies])
        df = pd.concat([df_2.reset_index(drop=True), df_3], axis=1)
        df['inspectiondate'] = pd.to_datetime(df.inspectiondate, format = '%m/%d/%Y')
        
        df['inspectionyear'] = df['inspectiondate'].dt.year
        
        df['inspection_month_name'] = df['inspectiondate'].dt.month_name()
        
        df['inspection_day_name'] = df['inspectiondate'].dt.day_name()
        df = df.drop(df.loc[df['inspection_day_name']== 'Saturday'].index)
        
        df = df.drop(df.loc[df['inspection_day_name']== 'Sunday'].index)
        df.rename(columns={'daycareid':'centerid'}, inplace=True)

        def order(frame,var): 
            varlist =[w for w in frame.columns if w not in var] 
            frame = frame[var+varlist] 
            return frame

        df = order(df,['centerid', 'inspectiondate'])
        df.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        df.violationcategory.value_counts(dropna=False)
        df['violation'] = df['violationcategory'].apply(lambda x: not pd.isnull(x))

        df['violation'] = df['violation'].apply(lambda x: 1 if x == True else 0)

        df.violation.value_counts(dropna=False)
        df['publichazard'] = df['violationcategory'].apply(lambda x: 1 if x == 'public_health_hazard' else 0)

        df.publichazard.value_counts(dropna=False)
        df['violaciones_hist_salud_publica'] = df.publichazard[(df.inspectionyear != 2020)]

        df_4 = df.groupby('centerid').violaciones_hist_salud_publica.sum().reset_index()

        df = pd.merge(left=df,right=df_4, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['violaciones_hist_salud_publica_x'], axis=1) #Eliminamos la variable repetida

        df.rename(columns={'violaciones_hist_salud_publica_y':'violaciones_hist_salud_publica'}, inplace=True)
        df['violaciones_2019_salud_publica'] = df.publichazard[(df.inspectionyear == 2019)]

        df_5 = df.groupby('centerid').violaciones_2019_salud_publica.sum().reset_index()

        df = pd.merge(left=df,right=df_5, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['violaciones_2019_salud_publica_x'], axis=1) #Eliminamos la variable repetida

        df.rename(columns={'violaciones_2019_salud_publica_y':'violaciones_2019_salud_publica'}, inplace=True)
        df['violationcritical'] = df['violationcategory'].apply(lambda x: 1 if x == 'critical' else 0)

        df['violaciones_hist_criticas'] = df.violationcritical[(df.inspectionyear != 2020)]

        df_6 = df.groupby('centerid').violaciones_hist_criticas.sum().reset_index()

        df = pd.merge(left=df,right=df_6, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['violaciones_hist_criticas_x'], axis=1) #Eliminamos la variable repetida

        df.rename(columns={'violaciones_hist_criticas_y':'violaciones_hist_criticas'}, inplace=True)
        df['violaciones_2019_criticas'] = df.violationcritical[(df.inspectionyear == 2019)]

        df_7 = df.groupby('centerid').violaciones_2019_criticas.sum().reset_index()

        df = pd.merge(left=df,right=df_7, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['violaciones_2019_criticas_x'], axis=1) #Eliminamos la variable repetida

        df.rename(columns={'violaciones_2019_criticas_y':'violaciones_2019_criticas'}, inplace=True)
        df_8 = df.loc[df['inspectionyear'] != 2020]

        df_9 = df_8[df_8.violationcategory.isin(['critical', 'public_health_hazard']) & df_8['initialannualinspection']==1]

        df_10 = df_9.groupby('centerid').initialannualinspection.sum().reset_index()

        df_11 = df.groupby('centerid').initialannualinspection.sum().reset_index()

        df_12 = pd.merge(left=df_11,right=df_10, how='left', left_on='centerid', right_on='centerid')

        df_12['ratio_violaciones_hist'] = df_12['initialannualinspection_y'] / df_12['initialannualinspection_x']

        df = pd.merge(left=df,right=df_12, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['initialannualinspection_x', 'initialannualinspection_y'], axis=1)
        df_13 = df.loc[df['inspectionyear'] == 2019]

        df_14 = df_13[df_13.violationcategory.isin(['critical', 'public_health_hazard']) & df_13['initialannualinspection']==1]

        df_15 = df_14.groupby('centerid').initialannualinspection.sum().reset_index()

        df_16 = pd.merge(left=df_11,right=df_15, how='left', left_on='centerid', right_on='centerid')

        df_16['ratio_violaciones_2019'] = df_16['initialannualinspection_y'] / df_16['initialannualinspection_x']

        df = pd.merge(left=df,right=df_16, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['initialannualinspection_x','initialannualinspection_y'], axis=1) #Eliminamos variables que no necesitamos
        df_17 = df.loc[df['inspectionyear'] != 2020]

        df_18 = df_17.groupby('borough').violation.mean().reset_index()

        df = pd.merge(left=df,right=df_18, how='left', left_on='borough', right_on='borough')

        df.rename(columns={'violation_y':'prom_violaciones_hist_borough'}, inplace=True)

        df.rename(columns={'violation_x':'violation'}, inplace=True)
        df_19 = df.loc[df['inspectionyear'] == 2019]

        df_20 = df_19.groupby('borough').violation.mean().reset_index()

        df = pd.merge(left=df,right=df_20, how='left', left_on='borough', right_on='borough')

        df.rename(columns={'violation_y':'prom_violaciones_2019_borough'}, inplace=True)

        df.rename(columns={'violation_x':'violation'}, inplace=True)
        df_21 = df.loc[df['inspectionyear'] != 2020]

        df_22 = df_21.loc[df_21['initialannualinspection'] == 1]

        df_23 = df_22.groupby('centerid').publichazard.sum().reset_index()

        df_24 = df_22.groupby('centerid').violation.sum().reset_index()

        df_25 = pd.merge(left=df_23,right=df_24, how='left', left_on='centerid', right_on='centerid')

        df_25['ratio_violaciones_hist_sp'] = df_25['publichazard'] / df_25['violation']

        df = pd.merge(left=df,right=df_25, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['publichazard_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos

        df.rename(columns={'violation_x':'violation'}, inplace=True)

        df.rename(columns={'publichazard_x':'publichazard'}, inplace=True)
        df_26 = df.loc[df['inspectionyear'] == 2019]

        df_27 = df_26.loc[df_26['initialannualinspection'] == 1]

        df_28 = df_27.groupby('centerid').publichazard.sum().reset_index()

        df_29 = df_27.groupby('centerid').violation.sum().reset_index()

        df_30 = pd.merge(left=df_28,right=df_29, how='left', left_on='centerid', right_on='centerid')

        df_30['ratio_violaciones_2019_sp'] = df_30['publichazard'] / df_30['violation']

        df = pd.merge(left=df,right=df_30, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['publichazard_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos

        df.rename(columns={'violation_x':'violation'}, inplace=True)

        df.rename(columns={'publichazard_x':'publichazard'}, inplace=True)
        df_31 = df.loc[df['inspectionyear'] != 2020]

        df_32 = df_31.loc[df_31['initialannualinspection'] == 1]

        df_33 = df_32.groupby('centerid').violationcritical.sum().reset_index()

        df_34 = df_32.groupby('centerid').violation.sum().reset_index()

        df_35 = pd.merge(left=df_33,right=df_34, how='left', left_on='centerid', right_on='centerid')

        df_35['ratio_violaciones_hist_criticas'] = df_35['violationcritical'] / df_35['violation']

        df = pd.merge(left=df,right=df_35, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['violationcritical_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos

        df.rename(columns={'violation_x':'violation'}, inplace=True)

        df.rename(columns={'violationcritical_x':'violationcritical'}, inplace=True)
        df_36 = df.loc[df['inspectionyear'] == 2019]

        df_37 = df_36.loc[df_36['initialannualinspection'] == 1]

        df_38 = df_37.groupby('centerid').violationcritical.sum().reset_index()

        df_39 = df_37.groupby('centerid').violation.sum().reset_index()

        df_40 = pd.merge(left=df_38,right=df_39, how='left', left_on='centerid', right_on='centerid')

        df_40['ratio_violaciones_2019_criticas'] = df_40['violationcritical'] / df_40['violation']

        df = pd.merge(left=df,right=df_40, how='left', left_on='centerid', right_on='centerid')

        df = df.drop(['violationcritical_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos

        df.rename(columns={'violation_x':'violation'}, inplace=True)

        df.rename(columns={'violationcritical_x':'violationcritical'}, inplace=True)
        df = df.set_index(['centerid', 'inspectiondate'])

        df = df.drop(df.columns[[0,1,2,3,4,5,6,7,8,9,11,12,13,14,15,17,19,21,23,24,25,26,27,56,57]], axis=1) #Eliminamos variables que no necesitamos
        df = df.fillna(0)

