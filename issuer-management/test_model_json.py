import unittest
from pilot_build_bundle import complete_response
from pilot_extract import parse_content

class ModelJSONTests(unittest.TestCase):
    def test_transport_success_requires_complete_object(self):
        def response(content,finish='stop'):
            return {'body':{'choices':[{'finish_reason':finish,'message':{'content':content}}]}}
        self.assertEqual(complete_response(response('{"candidates":[]}')),{'candidates':[]})
        for raw in (response('{"candidates":['),response('{}','length'),response('[]')):
            with self.assertRaises(ValueError):complete_response(raw)
    def test_redundant_closing_suffix_preserves_values(self):
        self.assertEqual(parse_content('{"raw_value":"128,441,668.45","candidates":[]}]}'),{'raw_value':'128,441,668.45','candidates':[]})
    def test_conflicting_second_object_rejected(self):
        with self.assertRaises(ValueError):parse_content('{"raw_value":"1"}{"raw_value":"2"}')
    def test_incomplete_object_rejected(self):
        with self.assertRaises(ValueError):parse_content('{"raw_value":"1"')

if __name__=='__main__':unittest.main()
