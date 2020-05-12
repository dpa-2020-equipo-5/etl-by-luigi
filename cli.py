from nyc_ccci_etl.metadata_helper.metadata_helper import MetadataHelper
r = MetadataHelper(2019,3,19)
print(r.get_inserted_raw_records())
print(r.get_inserted_raw_columns())
print(r.get_inserted_clean_records())
print(r.get_inserted_clean_columns())
print(r.get_inserted_transformed_records())
print(r.get_inserted_transformed_columns())