import luigi

from nyc_ccci_etl.luigi_tasks.load_clean_inspections_metadata_task import LoadCleanInspectionsMetadataTask
class NYCCCCIPipeline(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadCleanInspectionsMetadataTask(self.year, self.month, self.day)