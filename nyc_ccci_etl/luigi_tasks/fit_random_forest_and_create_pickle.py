import luigi
import luigi.contrib.s3
from nyc_ccci_etl.luigi_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.load_update_centers_metadata import LoadUpdateCentersMetadata
from nyc_ccci_etl.model.nyc_ccci_random_forest import NYCCCCIRandomForest
import pickle
class FitRandomForestAndCreatePickle(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadTransformedInspectionsMetadata(self.year, self.month, self.day), LoadUpdateCentersMetadata(self.year, self.month, self.day)

    def output(self):
        output_path = "s3://nyc-ccci/random_forest_{}_{}_{}.pckl".\
        format(
            str(self.year),
            str(self.month),
            str(self.day),
        )
        return luigi.contrib.s3.S3Target(path=output_path, format=luigi.format.Nop)

    def run(self):
        #random_forest = NYCCCCIRandomForest()
        #result = random_forest.execute()
        words = ['soy', 'un', 'random', 'forest']

        with self.output().open('w') as output_pickle:
            pickle.dump(words, output_pickle)
            #pickle.dump(result, output_pickle)