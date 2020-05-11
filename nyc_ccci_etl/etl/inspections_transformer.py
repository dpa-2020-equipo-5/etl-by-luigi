import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
import json
class InspectionsTransformer:
    def __init__(self, year, month, day):
        self.date_filter = "{}_{}_{}t00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
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
        #esto lo podemos cambiar para que crashee el tests
        df = pd.read_sql("select * from clean.inspections where inspectiondate='{}'".format(self.date_filter), self.engine)
        
        #quitar cmoentario para que falle el test
        #df = pd.read_sql("select * from clean.inspections", self.engine)

        tabla_4 = df.loc[:, ['borough', 'dc_id', 'inspectiondate', 'regulationsummary', 'violationcategory','healthcodesubsection', 'violationstatus', 'inspectionsummaryresult']]

        tabla_4['inspectionsummaryresult'] = tabla_4['inspectionsummaryresult'].astype('str')
        df_3 = pd.DataFrame(tabla_4.inspectionsummaryresult.str.split('___',1).tolist(), columns= ['reason', 'result'])
        df_3['result'] = df_3['result'].astype('str')
        df_4 = pd.DataFrame(df_3.result.str.split(';_',1).tolist(), columns = ['result_1', 'result_2'])
        df_3 = df_3.drop(df_3.columns[[1]], axis=1) 
        df_4 = df_4.join(df_3)
        tabla_4 = tabla_4.join(df_4)
        tabla_4 = tabla_4.drop(['inspectionsummaryresult'], axis = 1)

        tabla_4.reason.value_counts(dropna=False)
        tabla_4['initial_annual_inspection'] = tabla_4.reason.apply(lambda x: 1 if x == "initial_annual_inspection" else 0)
        tabla_4.initial_annual_inspection.value_counts(dropna=False)

        tabla_4.initial_annual_inspection.value_counts(dropna=False)
        tabla_4.drop(['reason'], axis=1, inplace=True) 
        
        dummies = ["result_1", "result_2"]
        df_2 = pd.get_dummies(tabla_4[dummies])
        tabla_4 = tabla_4.join(df_2)
        tabla_4 = tabla_4.drop(['result_1', 'result_2'], axis = 1) 

        
        tabla_4['inspectiondate'] = pd.to_datetime(tabla_4.inspectiondate, format = '%Y_%m_%dT00:00:00.000')

        tabla_4['inspection_year'] = tabla_4['inspectiondate'].dt.year
        tabla_4['inspection_month_name'] = tabla_4['inspectiondate'].dt.month_name()
        tabla_4['inspection_day_name'] = tabla_4['inspectiondate'].dt.day_name()
        
        tabla_4 = tabla_4.drop(tabla_4.loc[tabla_4['inspection_day_name']== 'Saturday'].index)
        tabla_4 = tabla_4.drop(tabla_4.loc[tabla_4['inspection_day_name']== 'Sunday'].index)
        
        tabla_4.rename(columns={'dc_id':'center_id'}, inplace=True)
        def order(frame,var): 
            varlist =[w for w in frame.columns if w not in var]
            frame = frame[var+varlist]
            return frame
        tabla_4 = order(tabla_4,['center_id', 'inspectiondate'])
        
        tabla_4.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        
        tabla_4.violationcategory.value_counts(dropna=False)
        tabla_4['violation'] = tabla_4['violationcategory'].apply(lambda x: not pd.isnull(x))
        tabla_4['violation'] = tabla_4['violation'].apply(lambda x: 1 if x == True else 0)
        tabla_4.violation.value_counts(dropna=False)
        
        tabla_4['public_hazard'] = tabla_4['violationcategory'].apply(lambda x: 1 if x == 'public_health_hazard' else 0)
        tabla_4.public_hazard.value_counts(dropna=False)

        tabla_4['violaciones_hist_salud_publica'] = tabla_4.public_hazard[(tabla_4.inspection_year != 2020)]
        df_4 = tabla_4.groupby('center_id').violaciones_hist_salud_publica.sum().reset_index()
        tabla_4 = pd.merge(left=tabla_4,right=df_4, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['violaciones_hist_salud_publica_x'], axis=1) #Eliminamos la variable repetida
        tabla_4.rename(columns={'violaciones_hist_salud_publica_y':'violaciones_hist_salud_publica'}, inplace=True)

        tabla_4['violaciones_2019_salud_publica'] = tabla_4.public_hazard[(tabla_4.inspection_year == 2019)]
        df_5 = tabla_4.groupby('center_id').violaciones_2019_salud_publica.sum().reset_index()
        tabla_4 = pd.merge(left=tabla_4,right=df_5, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['violaciones_2019_salud_publica_x'], axis=1) #Eliminamos la variable repetida
        tabla_4.rename(columns={'violaciones_2019_salud_publica_y':'violaciones_2019_salud_publica'}, inplace=True)

        tabla_4['violation_critical'] = tabla_4['violationcategory'].apply(lambda x: 1 if x == 'critical' else 0)
        tabla_4['violaciones_hist_criticas'] = tabla_4.violation_critical[(tabla_4.inspection_year != 2020)]
        df_6 = tabla_4.groupby('center_id').violaciones_hist_criticas.sum().reset_index()
        tabla_4 = pd.merge(left=tabla_4,right=df_6, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['violaciones_hist_criticas_x'], axis=1) #Eliminamos la variable repetida
        tabla_4.rename(columns={'violaciones_hist_criticas_y':'violaciones_hist_criticas'}, inplace=True)

        tabla_4['violaciones_2019_criticas'] = tabla_4.violation_critical[(tabla_4.inspection_year == 2019)]
        df_7 = tabla_4.groupby('center_id').violaciones_2019_criticas.sum().reset_index()
        tabla_4 = pd.merge(left=tabla_4,right=df_7, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['violaciones_2019_criticas_x'], axis=1) #Eliminamos la variable repetida
        tabla_4.rename(columns={'violaciones_2019_criticas_y':'violaciones_2019_criticas'}, inplace=True)


        df_8 = tabla_4.loc[tabla_4['inspection_year'] != 2020]
        df_9 = df_8[df_8.violationcategory.isin(['critical', 'public_health_hazard']) & df_8['initial_annual_inspection']==1]
        df_10 = df_9.groupby('center_id').initial_annual_inspection.sum().reset_index()
        df_11 = tabla_4.groupby('center_id').initial_annual_inspection.sum().reset_index()
        df_12 = pd.merge(left=df_11,right=df_10, how='left', left_on='center_id', right_on='center_id')
        df_12['ratio_violaciones_hist'] = df_12['initial_annual_inspection_y'] / df_12['initial_annual_inspection_x']
        tabla_4 = pd.merge(left=tabla_4,right=df_12, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['initial_annual_inspection_x', 'initial_annual_inspection_y'], axis=1) 

        df_13 = tabla_4.loc[tabla_4['inspection_year'] == 2019]
        df_14 = df_13[df_13.violationcategory.isin(['critical', 'public_health_hazard']) & df_13['initial_annual_inspection']==1]
        df_15 = df_14.groupby('center_id').initial_annual_inspection.sum().reset_index()
        df_16 = pd.merge(left=df_11,right=df_15, how='left', left_on='center_id', right_on='center_id')
        df_16['ratio_violaciones_2019'] = df_16['initial_annual_inspection_y'] / df_16['initial_annual_inspection_x']
        tabla_4 = pd.merge(left=tabla_4,right=df_16, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['initial_annual_inspection_x','initial_annual_inspection_y'], axis=1) #Eliminamos variables que no necesitamos 
                
        df_17 = tabla_4.loc[tabla_4['inspection_year'] != 2020]
        df_18 = df_17.groupby('borough').violation.mean().reset_index()
        tabla_4 = pd.merge(left=tabla_4,right=df_18, how='left', left_on='borough', right_on='borough')
        tabla_4.rename(columns={'violation_y':'prom_violaciones_hist_borough'}, inplace=True)
        tabla_4.rename(columns={'violation_x':'violation'}, inplace=True)
                
        df_19 = tabla_4.loc[tabla_4['inspection_year'] == 2019]
        df_20 = df_19.groupby('borough').violation.mean().reset_index()
        tabla_4 = pd.merge(left=tabla_4,right=df_20, how='left', left_on='borough', right_on='borough')
        tabla_4.rename(columns={'violation_y':'prom_violaciones_2019_borough'}, inplace=True)
        tabla_4.rename(columns={'violation_x':'violation'}, inplace=True)

        df_21 = tabla_4.loc[tabla_4['inspection_year'] != 2020]
        df_22 = df_21.loc[df_21['initial_annual_inspection'] == 1]
        df_23 = df_22.groupby('center_id').public_hazard.sum().reset_index()
        df_24 = df_22.groupby('center_id').violation.sum().reset_index()
        df_25 = pd.merge(left=df_23,right=df_24, how='left', left_on='center_id', right_on='center_id')
        df_25['ratio_violaciones_hist_sp'] = df_25['public_hazard'] / df_25['violation']
        tabla_4 = pd.merge(left=tabla_4,right=df_25, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['public_hazard_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 
        tabla_4.rename(columns={'violation_x':'violation'}, inplace=True)
        tabla_4.rename(columns={'public_hazard_x':'public_hazard'}, inplace=True)
                
        df_26 = tabla_4.loc[tabla_4['inspection_year'] == 2019]
        df_27 = df_26.loc[df_26['initial_annual_inspection'] == 1]
        df_28 = df_27.groupby('center_id').public_hazard.sum().reset_index()
        df_29 = df_27.groupby('center_id').violation.sum().reset_index()
        df_30 = pd.merge(left=df_28,right=df_29, how='left', left_on='center_id', right_on='center_id')


        df_30['ratio_violaciones_2019_sp'] = df_30['public_hazard'] / df_30['violation']
        tabla_4 = pd.merge(left=tabla_4,right=df_30, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['public_hazard_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 
        tabla_4.rename(columns={'violation_x':'violation'}, inplace=True)
        tabla_4.rename(columns={'public_hazard_x':'public_hazard'}, inplace=True)

        df_31 = tabla_4.loc[tabla_4['inspection_year'] != 2020]
        df_32 = df_31.loc[df_31['initial_annual_inspection'] == 1]
        df_33 = df_32.groupby('center_id').violation_critical.sum().reset_index()
        df_34 = df_32.groupby('center_id').violation.sum().reset_index()
        df_35 = pd.merge(left=df_33,right=df_34, how='left', left_on='center_id', right_on='center_id')
                
        df_35['ratio_violaciones_hist_criticas'] = df_35['violation_critical'] / df_35['violation']
        tabla_4 = pd.merge(left=tabla_4,right=df_35, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['violation_critical_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 
        tabla_4.rename(columns={'violation_x':'violation'}, inplace=True)
        tabla_4.rename(columns={'violation_critical_x':'violation_critical'}, inplace=True)

        df_36 = tabla_4.loc[tabla_4['inspection_year'] == 2019]
        df_37 = df_36.loc[df_36['initial_annual_inspection'] == 1]
        df_38 = df_37.groupby('center_id').violation_critical.sum().reset_index()
        df_39 = df_37.groupby('center_id').violation.sum().reset_index()
        df_40 = pd.merge(left=df_38,right=df_39, how='left', left_on='center_id', right_on='center_id')
        df_40['ratio_violaciones_2019_criticas'] = df_40['violation_critical'] / df_40['violation']
        tabla_4 = pd.merge(left=tabla_4,right=df_40, how='left', left_on='center_id', right_on='center_id')
        tabla_4 = tabla_4.drop(['violation_critical_y','violation_y'], axis=1) #Eliminamos variables que no necesitamos 
        tabla_4.rename(columns={'violation_x':'violation'}, inplace=True)
        tabla_4.rename(columns={'violation_critical_x':'violation_critical'}, inplace=True)
        return [tuple(x) for x in tabla_4.to_numpy()], [(c, 'VARCHAR') for c in list(tabla_4.columns)]  