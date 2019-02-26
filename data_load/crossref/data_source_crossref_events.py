import json
import xmltodict
from data_load.base.data_source import DataSource

from data_load.base.utils import file_utils

from data_load.crossref.api_crossref_events import CrossRefEventsAPI

class CrossrefEventsDataSource(DataSource):

    def __init__(self, doi_prefix, cursor_file_path, batch_size=1000, data_source_file_path=None):
        super(CrossrefEventsDataSource, self).__init__(data_source_file_path)
        self.doi_prefix = doi_prefix
        self.cursor_file_path = cursor_file_path
        self.batch_size = batch_size

    def process_rows(self, process_row_method):
        super(CrossrefEventsDataSource, self).process_rows(process_row_method)
    
        self.current_index = 0

        cursor_data = file_utils.load_file_path(self.cursor_file_path)
        cursor = None
        if 'cursor' in cursor_data:
            cursor = cursor_data['cursor']

        print 'Cursor file path:', self.cursor_file_path
        print 'Previous cursor:', cursor

        crossref_events_api = CrossRefEventsAPI()
        crossref_events_api.stream_events_for_doi_prefix(self.doi_prefix, self.works_fetched, cursor)
        
    def works_fetched(self, cursor, works):
        # existing_works = file_utils.load_file_path(self.data_source_file_path)
        # if len(existing_works) == 0:
        #     existing_works = []
        # existing_works.extend(works)
        # file_utils.save_file_path(self.data_source_file_path, existing_works)


        for doc in works:
            self.current_index += 1

            if not self.process_row_method(doc, self.current_index):
                break

            if self.current_index % self.batch_size == 0:
                print 'Saving cursor:', cursor
                cursor_data = {
                    'cursor': cursor
                }
                file_utils.save_file_path(self.cursor_file_path, cursor_data)
