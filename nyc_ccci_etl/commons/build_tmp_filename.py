import os
def build_tmp_filename(year, month, day):
    return os.environ['PYTHONPATH']  +   "/tmp/inspections_{}_{}_{}.json".format(str(year), str(month), str(day))