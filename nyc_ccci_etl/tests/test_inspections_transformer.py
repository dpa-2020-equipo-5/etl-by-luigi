from marbles.core import TestCase
from marbles.core.marbles import ContextualAssertionError
from nyc_ccci_etl.etl.inspections_transformer import InspectionsTransformer
from datetime import datetime

    
class TestInspectionsTransformer(TestCase):
    def test_transformed_length_equals_clean_length(self, year, month, day):
        try:
            it = InspectionsTransformer(year, month, day)
            ran_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            rows = it.execute()[0]
            with open('tmp/inserted_records_clean') as f:
                inserted_clean_records = int(f.read().strip())
            self.assertEqual(len(rows), inserted_clean_records, note="{} != {}. Se deben tranformar nada más las inspecciones del día".format(len(rows), inserted_clean_records))
            return {"test":"test_transformed_length_equals_clean_length", "status":"passed", "note": "Se deben tranformar nada más las inspecciones del día", "ran_at": ran_at}
        except ContextualAssertionError as e:
            return {"test":"test_transformed_length_equals_clean_length", "status":"failed", "note": e.note.strip() , "ran_at": ran_at}