
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
from json_schema import json_schema

from jsonmerge import Merger
import json
import time

class ValidateSchema(object):

    def __init__(self):
        self.schemas = []
        self.schema_errors = 0
        self.previous_doc = dict()

    def process(self, data_directory):
        xml_data_directory_source = XMLDataDirectorySource(data_directory, 'data_load/clinical_trials/clinical_trials_public.xsd')
        xml_data_directory_source.process_rows(self.process_row_method)

    def process_row_method(self, doc, index):
        # try:
        schema = {
            "properties": {
                "completion_date": {
                    "mergeStrategy": "overwrite"
                },
                "enrollment": {
                    "mergeStrategy": "overwrite"
                },
                "start_date": {
                    "mergeStrategy": "overwrite"
                },
                "primary_completion_date": {
                    "mergeStrategy": "overwrite"
                }
            }
        }

        merger = Merger(schema)
        self.previous_doc = merger.merge(self.previous_doc, doc)
        doc_json_string = json.dumps(self.previous_doc)
        # print doc_json_string
        match_found = False
        for schema in self.schemas:
            if json_schema.match(doc_json_string, schema):
                match_found = True
                break

        if not match_found:
            try:
                schema_for_doc = json_schema.dumps(doc_json_string)
                self.schemas.append(schema_for_doc)
            except Exception as e:
                print e.message
                self.schema_errors += 1
                # print doc_json_string
                # time.sleep(30)

        print 'Docs processed:', index+1, 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors

        # except Exception as e:
        #     print type(e)
        #     print e.args
        #     print e.message
        #     print ''
        #     print json.dumps(doc)
        #     print json.dumps(self.previous_doc)
        #     return False
       
        return True
        

validate_schema = ValidateSchema()
validate_schema.process('/Users/robin/Desktop/AllPublicXML/NCT0000xxxx')

