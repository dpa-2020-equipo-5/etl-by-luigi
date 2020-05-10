import sys
from nyc_ccci_etl.tests.test_inspections_extraction import TestInspectionsExtractor
test = TestInspectionsExtractor()

year = int(sys.argv[1])
month = int(sys.argv[2])
day = int(sys.argv[3])


r = test.test_extraction_date_is_valid(year, month, day)
if r['status'] == 'failed':
    print("\033[0;37;41m TEST FAILED:\033[0m \033[1;31;40m {}\033[0m".format("test_extraction_date_is_valid" ))
    print("\033[1;31;40mNote: {}\033[0m".format(r['note']))
    exit()
else:
    print("\033[0;37;42m TEST PASSED:\033[0m  {} ".format("test_extraction_date_is_valid" ))    

r = test.test_extraction_is_json(year, month, day)
print(r)
if r['status'] == 'failed':
    exit()

r = test.test_inspection_date_should_match_params_date(year, month, day)
print(r)
if r['status'] == 'failed':
    exit()

