from data_load.base.load_manager import LoadManager
from data_load.base.constants import DATA_LOADING_DIRECTORY
from data_load.crossref.crossref_data_mapper import CrossrefDataMapper
from data_load.crossref.crossref_data_extractor import CrossrefDataExtractor

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.crossref.data_source_crossref_works import CrossrefWorksDataSource

from data_load.grants import file_manager
from data_load.base.utils import file_utils

import os
import sys

from data_load.crossref.crossref_api import CrossRefAPI

ID_CROSSREF = "CROSSREF"


MODE_FILE = 'MODE_FILE'
MODE_AUTO = 'MODE_AUTO'
MODE_MEMBER = 'MODE_MEMBER'

class CrossrefLoadManager(LoadManager):

    def __init__(self, mode):
        super(CrossrefLoadManager, self).__init__(ID_CROSSREF)
        self.files_to_process = []
        self.mode = mode
        self.member_ids = []

    def get_root_directory(self, local_date):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() 

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "crossref",
            "index_type": "work"
        }

    def mapping_file_path(self):
        return 'data_load/crossref/mapping.json'

    def get_data_mapper(self):
        return CrossrefDataMapper()

    def get_data_extractor(self):
        return CrossrefDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []
        
        for member_id in self.member_ids:
            tasks_list.append({
                'name': str(member_id),
                'status': ''
            })
            
        return tasks_list

    def run_task(self, task):
        self.process(task)
            
    def download_data(self):
        load_config = self.get_load_config()
        
        # member_ids = []

        if self.mode == MODE_AUTO:
            self.member_ids = [5403, 297]
            # members = file_utils.load_file('/data_loading', 'crossref_members.json')
            # for member in members:
            #     member_id = member['id']
            #     member_ids.append(str(member_id))
        elif self.mode == MODE_MEMBER:
            self.member_ids.append(self.member_id)

        # crossref_api = CrossRefAPI()

        # print 'Total members:', len(self.member_ids)
        # for member_id in self.member_ids:
        #     file_name = 'crossref_works_member_' + member_id + '.json'
        #     results = file_utils.load_file(load_config.other_files_directory(), file_name)
        #     if len(results) == 0:
        #         results = crossref_api.get_works_for_member_id(member_id)
            
        #     data_file = load_config.other_files_directory() + '/' + file_name

        #     print 'Saving', len(results), 'works to', data_file
        #     file_utils.save_file(load_config.other_files_directory(), file_name, results)

        #     self.files_to_process.append(data_file)

    def process(self, member_id):
        # file_name = os.path.basename(data_source_file)

        data_source_batch_size = 1000

        load_config = self.get_load_config()
        load_config.data_source_name = 'crossref_works_member_' + str(member_id)
        load_config.data_source_batch_size = data_source_batch_size
        # load_config.process_count = 1

        cursor_file_path = load_config.other_files_directory() + '/' + 'next_cursor_works_for_member_id_' + str(member_id) + '.json'
        data_processor = DataSourceProcessor(load_config, CrossrefWorksDataSource(member_id, cursor_file_path, data_source_batch_size))
        data_processor.run()

def process_auto():
    load_manager = CrossrefLoadManager(MODE_AUTO)
    load_manager.del_config()
    load_manager.run()

def process_member(member_id):
    load_manager = CrossrefLoadManager(MODE_MEMBER)
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
                    print('Usage: crossref_load_manager -member <member_id>')     
            elif arg == '-auto': 
                process_auto()
            else:
                print('Usage: crossref_load_manager -auto')     
        arg_index += 1

if __name__ == '__main__':
    run()
