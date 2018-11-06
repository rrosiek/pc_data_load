import json
import xmltodict
from data_source import DataSource


class XMLDataSource(DataSource):

    def __init__(self, data_source_file_path, item_depth):
        super(XMLDataSource, self).__init__(data_source_file_path)
        self.item_depth = item_depth

    def process_rows(self, process_row_method):
        super(XMLDataSource, self).process_rows(process_row_method)
        self.current_index = 0
        xmltodict.parse(open(self.data_source_file_path), item_depth=self.item_depth, item_callback=self.handle_row)

    def handle_row(self, _, row):
        self.current_index += 1

        doc_json = json.dumps(row)
        doc_json = doc_json.replace('"@', '"')
        doc_json = doc_json.replace('#text', 'content')
        doc_json = doc_json.replace("\u2028", "")
        doc_json = doc_json.replace("\u2029", "")
        doc = json.loads(doc_json)

        if not self.process_row_method(doc, self.current_index):
            return False

        return True
