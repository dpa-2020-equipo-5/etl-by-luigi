import luigi

from .feature_engineering_validations.columns_one_hot_encoding_validation import ColumnsOneHotEncodingValidation
from .feature_engineering_validations.transformed_inspections_match_request_date_validation import TransformedInspectionsMatchRequestDateValidation
from nyc_ccci_etl.luigi_tasks.load_clean_inspections_metadata import LoadCleanInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.load_raw_inspections_metadata import LoadRawInspectionsMetadata

class FeatureEngineeringValidation(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    _complete = False
    
    def run(self):
        yield (
            TransformedInspectionsMatchRequestDateValidation(self.year, self.month, self.day),
            ColumnsOneHotEncodingValidation(self.year, self.month, self.day),
            LoadCleanInspectionsMetadata(self.year, self.month, self.day),
            LoadRawInspectionsMetadata(self.year, self.month, self.day)
        )
        self._complete = True

    def complete(self):
        return self._complete