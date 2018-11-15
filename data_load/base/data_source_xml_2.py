import json
import xmltodict
from data_source import DataSource
import xmlschema
import os

class XMLDataDirectorySource(object):

    def __init__(self, data_source_directory, schema_file_path):
        self.data_source_directory = data_source_directory
        self.schema_file_path = schema_file_path
        self.process_row_method = None

    def process_rows(self, process_row_method):
        self.process_row_method = process_row_method
        self.current_index = 0
        schema = xmlschema.XMLSchema(self.schema_file_path)

        for name in os.listdir(self.data_source_directory):
            file_path = os.path.join(self.data_source_directory, name)
            if os.path.isfile(file_path) and name.endswith('.xml'):
                if schema.is_valid(file_path):
                    doc = schema.to_dict(file_path)
                    if not self.process_row_method(doc, self.current_index):
                        break
                    self.current_index += 1

    def initialize(self):
        pass