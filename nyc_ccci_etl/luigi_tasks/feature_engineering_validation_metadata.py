import luigi
from .feature_engineering_validations.columns_one_hot_encoding_validation import ColumnsOneHotEncodingValidation

class FeatureEngineeringValidationMetadata(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    _complete = False
    
    def run(self):
        yield (
            ColumnsOneHotEncodingValidation(self.year, self.month, self.day)
        )
        self._complete = True

    def complete(self):
        return self._complete