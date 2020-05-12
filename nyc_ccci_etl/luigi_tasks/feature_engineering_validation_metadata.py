import luigi
from .feature_engineering_validation import FeatureEngineeringValidation
class FeatureEngineeringValidationMetadata(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    _complete = False
    
    def run(self):
        yield FeatureEngineeringValidation(self.year, self.month, self.day)
        self._complete = True

    def complete(self):
        return self._complete