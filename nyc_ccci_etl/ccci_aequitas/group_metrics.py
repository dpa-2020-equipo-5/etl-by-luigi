import pandas as pd
import boto3
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters
from aequitas.group import Group
class GroupMetrics:
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
    def download_model(self):
        ses = boto3.session.Session(profile_name='mathus_itam', region_name='us-east-1')
        latest_model = self.get_lastest_model(ses)
        s3_resource = ses.resource('s3')
        with BytesIO() as data:
            s3_resource.Bucket("nyc-ccci").download_fileobj(latest_model, data)
            data.seek(0)
            model = pickle.load(data)
        return model

    def execeute(self):
        model = self.download_model()
        tabla_3 = pd.read_sql_table('centers', self.engine, schema="transformed")
        tabla_4 = pd.read_sql_table('inspections', self.engine, schema="transformed")

        centros = tabla_3.copy()
        centros.rename(columns={"dc_id":"center_id"}, inplace=True)
        inspecciones = tabla_4.copy()
        last_inspections = inspecciones.sort_values(by="inspectiondate").drop_duplicates(subset=["center_id"], keep="last")
        centros = centros.drop(['centername', 'legalname', 'building', 'street', 'zipcode', 'phone', 'permitnumber', 'permitexp', 'status',  'agerange', 'childcaretype', 'bin', 'url', 'datepermitted', 'actual','violationratepercent','violationavgratepercent', 'publichealthhazardviolationrate','averagepublichealthhazardiolationrate','criticalviolationrate','avgcriticalviolationrate'], axis=1)
        centros = centros.reset_index(drop=True)
        tabla_5 = pd.merge(last_inspections, centros)
        tabla_5.sort_values(['inspectiondate'], ascending=[False], inplace=True)
        tabla_5['maximumcapacity'] = tabla_5['maximumcapacity'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['totaleducationalworkers'] = tabla_5['totaleducationalworkers'].astype(int)

        tabla_5['averagetotaleducationalworkers'] = tabla_5['averagetotaleducationalworkers'].astype(float)

        tabla_5 = tabla_5.drop(['regulationsummary', 'healthcodesubsection', 'violationstatus', 'borough', 'reason', 'inspectiondate', 'violationcategory_nan'], axis=1)

        tabla_5 = tabla_5.set_index(['center_id'])
        tabla_5 = tabla_5.fillna(0)

        for col in tabla_5.select_dtypes(object):
            tabla_5[col] = tabla_5[col].astype(float)

        tabla_5 = tabla_5.fillna(0)
        prds = model.predict(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))

        probas = model.predict_proba(tabla_5.drop(['violationcategory_public_health_hazard'],axis=1))

        res = pd.DataFrame({
            "center":tabla_5.index,
            "etiqueta":prds,
            "proba_0":probas[:,0],
            "proba_1":probas[:,1]
        })

        res.loc[res['proba_0'] > res['proba_1'], 'score'] = res['proba_0']
        res.loc[res['proba_0'] < res['proba_1'], 'score'] = res['proba_1']

        categorias_1 = ["programtype_all_age_camp","programtype_infant_toddler","programtype_preschool", "programtype_preschool_camp", "programtype_school_age_camp"]

        programtype = pd.get_dummies(centros[categorias_1]).idxmax(1)

        categorias_2 = ["borough_bronx","borough_brooklyn","borough_manhattan", "borough_queens", "borough_staten_island"]

        borough = pd.get_dummies(centros[categorias_2]).idxmax(1)

        ambas = pd.concat([borough, programtype], axis=1,)

        ambas = ambas.rename(columns={0:'borough', 1:'programtype'})

        centros = pd.concat([centros, ambas], axis=1)

        tabla = pd.merge(res, centros, left_on='center', right_on='center_id')

        tabla = tabla.loc[:, ['center', 'etiqueta', 'score', 'borough', 'programtype']]

        tabla =  tabla.rename(columns = {'etiqueta':'label_value'})

        tabla = tabla.set_index(['center'])
        g = Group()
        xtab, _ = g.get_crosstabs(tabla)
        absolute_metrics = g.list_absolute_metrics(xtab)
        df_group = xtab[[col for col in xtab.columns if col not in absolute_metrics]]
        self.output_table = df_group
        return [tuple(x) for x in df_group.to_numpy()], [(c, 'VARCHAR') for c in list(df_group.columns)]  
