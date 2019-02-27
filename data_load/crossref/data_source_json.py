import json
import xmltodict
from data_load.base.data_source import DataSource

from data_load.base.utils import file_utils

class JSONDataSource(DataSource):

    def __init__(self, data_source_file_path):
        super(JSONDataSource, self).__init__(data_source_file_path)

    def process_rows(self, process_row_method):
        super(JSONDataSource, self).process_rows(process_row_method)
        data = file_utils.load_file_path(self.data_source_file_path)
        
        self.current_index = 0
        for doc in data:
            if not self.process_row_method(doc, self.current_index):
                break
            self.current_index += 1
