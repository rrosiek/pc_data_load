from utils.logger import *
import os

class DataSource(object):

    def __init__(self, data_source_file_path):
        self.data_source_file_path = data_source_file_path
        self.data_source_file = None

        self.current_index = 0

        self.process_row_method = None

        file_name = os.path.basename(data_source_file_path)
        self.logger = Logger(file_name)

    def initialize(self):
        self.data_source_file_path = self.clean_file(self.data_source_file_path)

    # process_row gets a row, count
    def process_rows(self, process_row_method):
        # Implement in subclass
        self.process_row_method = process_row_method

    def open_data_file(self):
        try:
            if self.data_source_file is None:
                self.data_source_file = open(self.data_source_file_path, 'r')
        except Exception as e:
            print e.message

    def close_data_file(self):
        if self.data_source_file is not None:
            self.data_source_file.close()
            self.data_source_file = None

    def clean_file(self, data_source_file_path):
        # Implement in subclass
        # print 'parent class clean_file'
        return data_source_file_path
