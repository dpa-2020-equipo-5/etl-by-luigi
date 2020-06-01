import luigi
from .predictions_validations.predictions_columns_validation import PredictionsColumnsValidation
from .predictions_validations.at_least_one_center_validation import AtLeastOneCenterValidation
class PredictionsValidationMetadata(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    matrix_uuid = luigi.Parameter()
    _complete = False
    
    def run(self):
        yield (
            PredictionsColumnsValidation(self.year, self.month, self.day, self.matrix_uuid),
            AtLeastOneCenterValidation(self.year, self.month, self.day, self.matrix_uuid)
        )
        self._complete = True

    def complete(self):
        return self._complete
    
    