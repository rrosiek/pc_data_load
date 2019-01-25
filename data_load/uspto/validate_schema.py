
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
from data_load.xsdtojson.lib import xsd_to_json_schema
from data_load.base.utils import file_utils

import xmltodict
import json
import time
import os

from jsonschema import validate

class ValidateSchema(object):

    def __init__(self):
        self.schemas = []
        self.schema_errors = 0
        self.index = 0

        self.format = {}

    def create_mapping(self, data_directory, format):
        mapping = {}
        for signature in format:
            format_item = format[signature]
            parents = format_item['parents']

            mapping = self.add_mapping(parents, mapping)

        file_utils.save_file(data_directory, 'mapping_generated.json', mapping)

    def add_mapping(self, parents, mapping):
        child_mapping = {}
        if len(parents) == 1:
            child_mapping = {
                "type": "text"
            }
            key = parents.pop()
            
            # if key not in ['description', 'abstract', 'claims', 'us-sequence-list-doc']:
            mapping[key] = child_mapping
            return mapping
        else: 
            key = parents.pop(0)
            # key = parents[0]
            # parents = parents[:-1]
            print key
            properties = {}
            if key in mapping:
                if 'properties' in mapping[key]:
                    properties = mapping[key]['properties']
                # properties = mapping[key]['properties']
            child_mapping = {
                "properties" : self.add_mapping(parents, properties)
            }
            # if key not in ['description', 'abstract', 'claims', 'us-sequence-list-doc']:
            mapping[key] = child_mapping
            return mapping  

    def process(self, data_directory):
        for name in os.listdir(data_directory):
            file_path = os.path.join(data_directory, name)
            if os.path.isfile(file_path) and name.endswith('.xml'):
                print 'Parsing file:', file_path
                fo = open(file_path, 'r')
                line = fo.readline()
                doc = []
                docs = []
                while line != '':
                    doc.append(line)
                    line = fo.readline()
                    if line.strip() == '<?xml version="1.0" encoding="UTF-8"?>':
                        # print line
                        doc_str = ' '.join(doc)
                        # print doc_str
                        doc_str_json = xmltodict.parse(doc_str)
                        self.append_format([], doc_str_json, self.format)

                        docs.append(doc_str_json)
                        # print doc_str_json
                        doc = []
                        # doc.append(line)
                        print len(docs), 'Docs'

                
                print 'Parsing file:', file_path
                print len(docs), 'Docs'
                # xmltodict.parse(open(file_path), item_depth=1, item_callback=self.handle_row)

        file_utils.save_file(data_directory, 'schema.json', self.format)
        self.create_mapping(data_directory, self.format)

    # def handle_row(self, _, row):
    #     self.append_format([], row, self.format)

    #     self.index += 1
    #     print 'Docs processed:', self.index , 'Keys in format:', len(self.format), 'Schema errors:', self.schema_errors
    #     return True

    def append_format(self, parents, data, format):      
        if isinstance(data, dict):
            for key in data:
                data_item = data[key]

                parents_copy = parents[:]
                parents_copy.append(key)

                self.append_format(parents_copy, data_item, format)
        elif isinstance(data, list):
            for item in data:
                self.append_format(parents, item, format)
        else:
            signature = '_'.join(parents)
            if signature not in format:
                format[signature] = {
                    "parents": parents,
                    "data": [data]
                }  

validate_schema = ValidateSchema()
validate_schema.process('/data/data_loading/source-files/uspto/')