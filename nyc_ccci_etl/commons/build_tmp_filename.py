import os
def build_tmp_filename(year, month, day):
    return "tmp/inspections_{}_{}_{}.json".format(str(year), str(month), str(day))