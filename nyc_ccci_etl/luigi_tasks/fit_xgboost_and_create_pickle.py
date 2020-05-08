import luigi
import luigi.contrib.s3
from nyc_ccci_etl.luigi_tasks.load_transformed_inspections_metadata import LoadTransformedInspectionsMetadata
from nyc_ccci_etl.luigi_tasks.load_update_centers_metadata import LoadUpdateCentersMetadata
from nyc_ccci_etl.model.nyc_ccci_xg_boost import NYCCCCIXGBoost
import pickle
class FitXGBoostAndCreatePickle(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadTransformedInspectionsMetadata(self.year, self.month, self.day), LoadUpdateCentersMetadata(self.year, self.month, self.day)

    def output(self):
        output_path = "s3://nyc-ccci/xg_boost_{}_{}_{}.pckl".\
        format(
            str(self.year),
            str(self.month),
            str(self.day),
        )
        return luigi.contrib.s3.S3Target(path=output_path, format=luigi.format.Nop)

    def run(self):
        xg_boost = NYCCCCIXGBoost()
        result = xg_boost.execute()

        with self.output().open('w') as output_pickle:
            pickle.dump(result, output_pickle)