from sodapy import Socrata
from nyc_ccci_etl.commons.configuration import get_app_token

class InspectionsExtractor():
    def __init__(self, year, month, day):
        self.date_param = "{}-{}-{}T00:00:00.000".format(year, month,day)
        self.socrataClient = Socrata("data.cityofnewyork.us", get_app_token()) 

    def execute(self):
        return self.socrataClient.get("dsg6-ifza", limit=5000)#, inspectiondate=self.date_param)