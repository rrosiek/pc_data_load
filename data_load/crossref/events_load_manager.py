from data_load.base.load_manager import LoadManager
from data_load.base.constants import DATA_LOADING_DIRECTORY
from data_load.crossref.events_data_mapper import EventsDataMapper
from data_load.crossref.events_data_extractor import EventsDataExtractor

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.crossref.data_source_crossref_events import CrossrefEventsDataSource

from data_load.grants import file_manager
from data_load.base.utils import file_utils

import os
import sys

from data_load.crossref.api_crossref import CrossRefAPI
from data_load.crossref.api_crossref_events import CrossRefEventsAPI

ID_CROSSREF_EVENTS = "CROSSREF_EVENTS"


MODE_FILE = 'MODE_FILE'
MODE_AUTO = 'MODE_AUTO'
MODE_MEMBER = 'MODE_MEMBER'

class EventsLoadManager(LoadManager):

    def __init__(self, mode):
        super(EventsLoadManager, self).__init__(ID_CROSSREF_EVENTS)
        self.files_to_process = []
        self.mode = mode
        self.member_ids = []
        self.doi_prefixes = []

    def get_root_directory(self, local_date):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() 

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "crossref_events",
            "index_type": "event"
        }

    def mapping_file_path(self):
        return 'data_load/crossref/mapping.json'

    def get_data_mapper(self):
        return EventsDataMapper()

    def get_data_extractor(self):
        return EventsDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []
        
        for doi_prefix in self.doi_prefixes:
            tasks_list.append({
                'name': doi_prefix,
                'status': ''
            })
            
        return tasks_list

    def run_task(self, task):
        self.process(task)
            
    def download_data(self):
        load_config = self.get_load_config()
        
        # file_manager.download_files(load_config)
        # member_ids = []

        if self.mode == MODE_AUTO:
            self.member_ids = [5403, 297]
            
        elif self.mode == MODE_MEMBER:
            self.member_ids = [self.member_id]

        crossref_api = CrossRefAPI()
        self.doi_prefixes = crossref_api.get_doi_prefixes_for_member_ids(self.member_ids)

        # events_api = CrossRefEventsAPI()

        # for doi_prefix in doi_prefixes:
        #     file_name = 'crossref_events_doi_prefix_' + doi_prefix + '.json'

        #     events = file_utils.load_file(load_config.other_files_directory(), file_name)
        #     if len(events) == 0:
        #         events = events_api.get_events_for_doi_prefix(doi_prefix)
           
        #     data_file = load_config.other_files_directory() + '/' + file_name
        #     print 'Saving', len(events), 'events to', data_file

        #     file_utils.save_file(load_config.other_files_directory(), file_name, events)
        #     self.files_to_process.append(data_file)

    def process(self, doi_prefix):
        # file_name = os.path.basename(data_source_file)

        data_source_batch_size = 1000
        load_config = self.get_load_config()
        load_config.data_source_name = 'crossref_events_doi_prefix_' + doi_prefix
        load_config.data_source_batch_size = data_source_batch_size

        cursor_file_path = load_config.other_files_directory() + '/' + 'next_cursor_events_for_doi_prefix_' + str(doi_prefix) + '.json'
        data_processor = DataSourceProcessor(load_config, CrossrefEventsDataSource(doi_prefix, cursor_file_path, data_source_batch_size))
        data_processor.run()

def process_auto():
    load_manager = EventsLoadManager(MODE_AUTO)
    load_manager.del_config()
    load_manager.run()

def process_member(member_id):
    load_manager = EventsLoadManager(MODE_MEMBER)
    load_manager.del_config()
    load_manager.member_id = member_id
    load_manager.run()

def run():
    arg_index = 0
    for arg in sys.argv:
        if arg_index > 0:
            if arg == '-member':
                if (arg_index + 1) < len(sys.argv):
                    member_id = sys.argv[arg_index + 1]
                    process_member(member_id)   
                    return
                else:
                    print('Usage: events_load_manager -member <member_id>')     
            elif arg == '-auto': 
                process_auto()
            else:
                print('Usage: events_load_manager -auto')     
        arg_index += 1

if __name__ == '__main__':
    run()
