import luigi
from luigi.contrib.postgres import CopyToTable
from nyc_ccci_etl.orchestrator_tasks.feature_engineering_validation_metadata import FeatureEngineeringValidationMetadata
from nyc_ccci_etl.predict.predictions_creator import PredictionsCreator
import boto3
import pickle
from io import BytesIO
class CreatePredictions(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()
    _complete = False
    def requires(self):
        return FeatureEngineeringValidationMetadata(self.year, self.month, self.day)

    def run(self):
        ses = boto3.session.Session(profile_name='mathus_itam', region_name='us-east-1')
        s3_resource = ses.resource('s3')
        
        with BytesIO() as data:
            s3_resource.Bucket("nyc-ccci").download_fileobj("random_forest_{}_{}_{}.pckl".format(self.year, self.month, self.day), data)
            data.seek(0)
            model = pickle.load(data)
        
        predictor = PredictionsCreator(model)
        predictions = predictor.create_predictions()
        print(predictions)


        self._complete = True
        
    def complete(self):
        return self._complete