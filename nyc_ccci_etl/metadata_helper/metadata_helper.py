import pandas as pd
from sqlalchemy import create_engine
from nyc_ccci_etl.commons.configuration import get_database_connection_parameters

class MetadataHelper:
    def __init__(self, year, month, day):
        host, database, user, password = get_database_connection_parameters()
        self.date_filter = "{}-{}-{}T00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        self.date_filter_clean = "{}_{}_{}t00:00:00.000".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        self.date_filter_transformed = "{}-{}-{} 00:00:00".format(str(year).zfill(2), str(month).zfill(2), str(day).zfill(2))
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user = user,
            password = password,
            host = host,
            port = 5432,
            database = database,
        )
        self.engine = create_engine(engine_string)

    def get_inserted_raw_records(self):
        df = pd.read_sql("select count(*) from raw.inspections", self.engine)
        return df['count'].values[0]
        
    def get_inserted_raw_columns(self):
        df = pd.read_sql("select * from raw.inspections limit 1;", self.engine)
        return ",".join(df['inspection'].values[0].keys())

    def get_inserted_clean_records(self):
        df = pd.read_sql("select count(*) from clean.inspections;", self.engine)
        return df['count'].values[0]
        
    def get_inserted_clean_columns(self):
        df = pd.read_sql("select * from clean.inspections limit 1;", self.engine)
        return ",".join(df.columns)
    
    def get_inserted_transformed_records(self):
        df = pd.read_sql("select count(*) from transformed.inspections;", self.engine)
        return df['count'].values[0]
        
    def get_inserted_transformed_columns(self):
        df = pd.read_sql("select * from transformed.inspections limit 1;", self.engine)
        return ",".join(df.columns)