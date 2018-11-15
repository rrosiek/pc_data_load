
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
from json_schema import json_schema
from json_schema.json_differ import diff_jsons
from data_load.xsdtojson.lib import xsd_to_json_schema

from jsonmerge import Merger
import json
import time

from jsonschema import validate

class ValidateSchema(object):

    def __init__(self):
        self.schemas = []
        self.schema_errors = 0
        self.previous_doc = dict()

    def process(self, data_directory):
        root_schema = xsd_to_json_schema('data_load/clinical_trials/clinical_trials_public.xsd')
        root_schema_obj = json.loads(root_schema)
        # root_schema_string = json.dumps(root_schema_obj)
        # root_schema_string = root_schema_string.replace('\n', '')
        # root_schema_string = root_schema_string.replace('\"', '"')

        # print root_schema_string
        self.schemas.append(root_schema_obj)
        xml_data_directory_source = XMLDataDirectorySource(data_directory, 'data_load/clinical_trials/clinical_trials_public.xsd')
        xml_data_directory_source.process_rows(self.process_row_method)

    def process_row_method(self, doc, index):
        if 'rank' not in doc:
            doc['rank'] = ""
        if 'variable_date_struct' not in doc:
            doc['variable_date_struct'] = ""
        doc_json_string = json.dumps(doc)
        for schema in self.schemas:
            # my_schema_object = json_schema.loads(schema)
            # my_schema_object.full_check(doc_json_string)
            # if json_schema.match(doc_json_string, schema):
            #     match_found = True
            # else:
            #     self.schema_errors += 1
            try:
                validate(doc, schema)
            except Exception as e:
                print e.message
                print e.args
                print type(e)
                self.schema_errors += 1
        print 'Docs processed:', index+1, 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors
        return True

    # def process_row_method(self, doc, index):
    #     # try:
    #     schema_root = {
    #         "properties": {
    #             "completion_date": {
    #                 "mergeStrategy": "overwrite"
    #             },
    #             "enrollment": {
    #                 "mergeStrategy": "overwrite"
    #             },
    #             "start_date": {
    #                 "mergeStrategy": "overwrite"
    #             },
    #             "primary_completion_date": {
    #                 "mergeStrategy": "overwrite"
    #             }
    #         }
    #     }

    #     merger = Merger(schema_root)
    #     self.previous_doc = merger.merge(self.previous_doc, doc)
    #     self.normalize_array(self.previous_doc)
    #     doc_json_string = json.dumps(self.previous_doc)
    #     # print doc_json_string
    #     match_found = False
        # for schema in self.schemas:
        #     if json_schema.match(doc_json_string, schema):
        #         match_found = True
        #         break

    #     if not match_found:
    #         try:
    #             schema_for_doc = json_schema.dumps(doc_json_string)
    #             self.schemas.append(schema_for_doc)
    #         except Exception as e:
                # print e.message
                # print e.args
                # print type(e)
    #             self.schema_errors += 1
    #             print doc_json_string
    #             exit()

    #     print 'Docs processed:', index+1, 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors

    #     # except Exception as e:
    #     #     print type(e)
    #     #     print e.args
    #     #     print e.message
    #     #     print ''
    #     #     print json.dumps(doc)
    #     #     print json.dumps(self.previous_doc)
    #     #     return False
       
    #     return True
        

    def normalize_array(self, doc):
        if isinstance(doc, list):
            keys = []
            for item in doc:
                if isinstance(item, dict):
                    for key in item:
                        if key not in keys:
                            keys.append(key)

            for item in doc:
                if isinstance(item, dict):
                    for key in keys:
                        if key not in item:
                            item[key] = None
                self.normalize_array(item)                    
        elif isinstance(doc, dict):
            for key in doc:
                item = doc[key]
                self.normalize_array(item)
            
    


validate_schema = ValidateSchema()
validate_schema.process('/Users/robin/Desktop/AllPublicXML/NCT0000xxxx')

