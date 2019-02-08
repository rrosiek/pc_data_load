
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
from data_load.xsdtojson.lib import xsd_to_json_schema
from data_load.base.utils import file_utils

import xmltodict
import json
import time
import os
import pprint
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

    def process_mapping(self):
        mapping = {}
        with open('data_load/uspto/mapping.json') as mapping_file:
            mapping = json.load(mapping_file)
            sub_mapping = mapping['mappings']['grant']['properties']['us-patent-grant']['properties']

            # print mapping
            self.print_mapping(sub_mapping, 0, '')

            mapping_new = {}
            for key in sub_mapping:
                if key in ['@status',
                            '@country',
                            '@lang',
                            '@dtd-version',
                            '@date-publ',
                            '@date-produced',
                            'drawings',
                            '@file' ,
                            'us-math', 
                            'us-sequence-list-doc_str',
                            'us-bibliographic-data-grant',
                            '@id',
                            'us-chemistry',
                            'us-claim-statement'
                            ]:
                    data = sub_mapping[key]
                    mapping_new[key] = data

        # '@status',
        # 'description ',
        # '@country',
        # '@lang',
        # 'abstract',
        # '@dtd-version',
        # '@date-publ',
        # '@date-produced',
        # 'drawings',
        # '@file' ,
        # 'us-math', 
        # 'claims' ,
        # 'us-sequence-list-doc_str',
        # 'us-bibliographic-data-grant',
        # '@id',
        # 'us-chemistry',
        # 'us-claim-statement'

            mapping['mappings']['grant']['properties']['us-patent-grant']['properties'] = mapping_new

        file_utils.save_file('data_load/uspto/', 'mapping.json', mapping)


            # properties = mapping['mappings']['grant']['properties']

            # for key in properties:
            #     print key

            # keys_to_print = ['sequence-cwu', 'us-patent-grant']
            # for key in keys_to_print:
            #     print key
            #     properties = mapping['mappings']['grant']['properties'][key]['properties']
            #     # pp = pprint.PrettyPrinter(indent=1)
            #     # print(json.dumps(properties, indent=2, sort_keys=True)) 
            #     for key in properties:
            #         data_for_key = properties[key]
            #         print '       ', key, len(json.dumps(data_for_key))
            #         if key in ['description', 'abstract', 'claims', 'us-bibliographic-data-grant']:
            #             for k in data_for_key['properties']:
            #                 print '          ',k
                            # print(json.dumps(data_for_key['properties'][k], indent=2, sort_keys=True)) 
            # properties = mapping['mappings']['grant']['properties']['sequence-cwu']['properties']
            # pp = pprint.PrettyPrinter(indent=1)
            # print(json.dumps(properties, indent=2, sort_keys=True)) 


    def print_mapping(self, mapping, indent, parents):
        if indent > 0:
            return
        if 'properties' in mapping:
            properties = mapping['properties']
            self.print_mapping(properties, indent, parents)
        else:
            if isinstance(mapping, dict):
                for key in mapping:
                    child_mapping = mapping[key]
                    if len(parents) == 0:
                        parents_sub = key
                    else:
                        parents_sub = parents + '.' + key
                    if key != 'type' and parents_sub.startswith(''):
                        print self.get_indent(indent) + key, '(' + str(len(json.dumps(child_mapping))) + ')'
                    self.print_mapping(child_mapping, indent + 1, parents_sub)
            elif isinstance(mapping, list):
                for item in mapping:
                    self.print_mapping(item, indent + 1, parents)
            else:
                pass
                # print self.get_indent(indent) + mapping

    def get_indent(self, indent):
        indent_str = ''
        while indent > 0:
            indent_str += '|  '
            indent = indent - 1

        return indent_str

validate_schema = ValidateSchema()
# validate_schema.process('/data/data_loading/source-files/uspto/')
validate_schema.process_mapping()