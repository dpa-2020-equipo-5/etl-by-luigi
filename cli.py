import sys
from nyc_ccci_etl.tests.test_inspections_transformer import TestInspectionsTransformer
from nyc_ccci_etl.utils.print_with_format import print_test_failed, print_test_passed
t = TestInspectionsTransformer()
result = t.test_transformed_length_equals_clean_length(int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]))
if result['status'] == 'failed':
    print_test_failed(result['test'], result['note'])
else:
    print_test_passed(result['test'])