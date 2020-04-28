import luigi
from .load_clean_inspections_task import LoadCleanInspectionsTask

class CleanStage(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return LoadCleanInspectionsTask(self.year, self.month, self.day)
