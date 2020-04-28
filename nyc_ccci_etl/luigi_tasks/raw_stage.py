import luigi
from .load_raw_inspections_metadata_task import LoadRawInspectionsMetadataTask

class RawStage(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadRawInspectionsMetadataTask(self.year, self.month, self.day)
