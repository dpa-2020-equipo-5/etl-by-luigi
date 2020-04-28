import luigi

from .raw_stage import RawStage
from .clean_stage import CleanStage
class NYCCCCIPipeline(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    day = luigi.IntParameter()

    def requires(self):
        return CleanStage(self.year, self.month, self.day)
        