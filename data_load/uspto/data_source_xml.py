import json
from data_load.base.data_source import DataSource
# import xmlschema
import os
import xmltodict

class XMLDataSource(DataSource):

    def __init__(self, data_source_file_path, item_depth):
        super(XMLDataSource, self).__init__(data_source_file_path)
        self.item_depth = item_depth

    def process_rows(self, process_row_method):
        self.process_row_method = process_row_method
        self.current_index = 0

        print 'Parsing file:', self.data_source_file_path
        fo = open(self.data_source_file_path, 'r')
        line = fo.readline()

        doc = []

        while line != '':
            doc.append(line)
            line = fo.readline()
            if line.strip() == '<?xml version="1.0" encoding="UTF-8"?>':
                self.current_index += 1

                doc_str_xml = ' '.join(doc)
                doc_str_json = xmltodict.parse(doc_str_xml)

                if not self.process_row_method(doc_str_json, self.current_index):
                    break
                # print self.current_index, 'docs processed'
                doc = []

    def initialize(self):
        pass