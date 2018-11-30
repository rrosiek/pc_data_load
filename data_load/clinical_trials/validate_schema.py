
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
# from json_schema import json_schema
# from json_schema.json_differ import diff_jsons
from data_load.xsdtojson.lib import xsd_to_json_schema

# from jsonmerge import Merger

import xmltodict
import json
import time
import os

from jsonschema import validate

class ValidateSchema(object):

    def __init__(self):
        self.schemas = []
        self.schema_errors = 0
        self.previous_doc = dict()
        self.index = 0
        self.exisiting_format = {}

    def process(self, data_directory):
        for name in os.listdir(data_directory):
            file_path = os.path.join(data_directory, name)
            if os.path.isfile(file_path) and name.endswith('.xml'):
                print 'Parsing file:', file_path
                xmltodict.parse(open(file_path), item_depth=1, item_callback=self.handle_row)

    def handle_row(self, _, row):
        root_schema = xsd_to_json_schema('data_load/clinical_trials/clinical_trials_public.xsd')
        schema = json.loads(root_schema)
        self.existing_format = self.validate_json(self.exisiting_format, row)
        try:
            validate(row, schema)
        except Exception as e:
            print e.message
            print e.args
            print type(e)
            self.schema_errors += 1

        self.index += 1
        print 'Docs processed:', self.index , 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors
        return True

    def combine_items(self, old_item, new_item):
        if isinstance(old_item, list) and isinstance(new_item, list):
            return self.combine_lists(old_item, new_item)
        elif isinstance(old_item, dict) and isinstance(new_item, dict):
            return self.combine_dictionary(old_item, new_item)
        elif isinstance(old_item, dict) and isinstance(new_item, list):
            return self.combine_lists([old_item], new_item)
        elif isinstance(old_item, list) and isinstance(new_item, dict):
            return self.combine_lists(old_item, [new_item])

    def combine_dictionary(self, old_item, new_item):
        combined = {}
        for key in old_item:
            combined[key] = old_item[key]

        for key in new_item:
            new_item_for_key = new_item[key]
            if key in combined:
                old_item_for_key = combined[key]

                combined_item_for_key = self.combine_items(old_item_for_key, new_item_for_key)
                combined[key] = combined_item_for_key
            else:
                combined[key] = new_item_for_key

        return combined  

    def combine_lists(self, old_item, new_item):
        pre_combined_list = []
        pre_combined_list.extend(old_item)
        pre_combined_list.extend(new_item)

        combined = []
        for item in pre_combined_list:
            if len(combined) == 0:
                combined.append(item)
            else:
                first_item = combined[0]
                combined_item = self.combine_items(first_item, item)
                combined[0] = combined_item

        return combined   

    def validate_json(self, old_item, new_item):
        # Determine type of old and new item

        # If either is a list, iterate and combine objects in each array, then combine objects with each other
        list_one = []
        list_two = []
        if isinstance(old_item, list):
            list_one = old_item
        else:
            list_one = [old_item]

        if isinstance(new_item, list):
            list_two = new_item
        else:
            list_two = [new_item]
        
        # If both are dict, combine them
        




    # def validate_json(self, existing_format, new_json):
    #     for key in new_json:
    #         ef_item = {}
    #         if key in existing_format:
    #             ef_item = existing_format[key]
            
    #         new_item = new_json[key]
    #         if isinstance(ef_item, dict) and isinstance(new_item, dict):
    #             ef_item = self.validate_json(ef_item, new_item)
    #         elif isinstance(new_item, list):
    #             if isinstance(ef_item, list):
    #                 if len(ef_item) > 0:
    #                     ef_item = ef_item[0]
    #                 else:
    #                     ef_item = {}
    #                 for item in new_item:
    #                     ef_item = self.validate_json(ef_item, item)
    #             elif isinstance(ef_item, dict):
    #                  for item in new_item:
    #                     ef_item = self.validate_json(ef_item, item)
    #             else:
    #                 ef_item = [new_item[0]]
                       
            
            


    #         existing_format[key] = ef_item



    # def process(self, data_directory):
    #     root_schema = xsd_to_json_schema('data_load/clinical_trials/clinical_trials_public.xsd')
    #     root_schema_obj = json.loads(root_schema)
    #     # root_schema_string = json.dumps(root_schema_obj)
    #     # root_schema_string = root_schema_string.replace('\n', '')
    #     # root_schema_string = root_schema_string.replace('\"', '"')

    #     # print root_schema_string
    #     self.schemas.append(root_schema_obj)
    #     xml_data_directory_source = XMLDataDirectorySource(data_directory, 'data_load/clinical_trials/clinical_trials_public.xsd')
    #     xml_data_directory_source.process_rows(self.process_row_method)

    # def process_row_method(self, doc, index):
    #     if 'rank' not in doc:
    #         doc['rank'] = ""
    #     if 'variable_date_struct' not in doc:
    #         doc['variable_date_struct'] = ""
    #     doc_json_string = json.dumps(doc)
    #     for schema in self.schemas:
    #         # my_schema_object = json_schema.loads(schema)
    #         # my_schema_object.full_check(doc_json_string)
    #         # if json_schema.match(doc_json_string, schema):
    #         #     match_found = True
    #         # else:
    #         #     self.schema_errors += 1
            # try:
            #     validate(doc, schema)
            # except Exception as e:
            #     print e.message
            #     print e.args
            #     print type(e)
            #     self.schema_errors += 1
    #     print 'Docs processed:', index+1, 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors
    #     return True

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

