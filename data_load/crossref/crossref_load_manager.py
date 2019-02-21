from data_load.base.load_manager import LoadManager
from data_load.base.constants import DATA_LOADING_DIRECTORY
from data_load.crossref.crossref_data_mapper import CrossrefDataMapper
from data_load.crossref.crossref_data_extractor import CrossrefDataExtractor

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.crossref.data_source_json import JSONDataSource

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
        self.member_id = None

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
        
        for file_to_process in self.files_to_process:
            tasks_list.append({
                'name': file_to_process,
                'status': ''
            })
            
        return tasks_list

    def run_task(self, task):
        self.process(task)
            
    def download_data(self):
        load_config = self.get_load_config()
        
        # file_manager.download_files(load_config)

        if self.mode == MODE_AUTO:
            member_ids = []
            members = file_utils.load_file('/data_loading', 'crossref_members.json')
            for member in members:
                member_id = member['id']
                member_ids.append(str(member_id))

            print 'Total members:', len(member_ids)
            for member_id in member_ids:
                crossref_api = CrossRefAPI()
                results = crossref_api.get_works_for_member_id(member_id)
                # print 'Total results:', len(results)

                file_name = 'crossref_works_member_' + member_id + '.json'
                file_utils.save_file(load_config.other_files_directory(), file_name, results)
                data_file = load_config.other_files_directory() + '/' + file_name

                self.files_to_process.append(data_file)

        elif self.mode == MODE_MEMBER:
            crossref_api = CrossRefAPI()
            results = crossref_api.get_works_for_member_id(self.member_id)
            print 'Total results:', len(results)

            file_name = 'crossref_works_member_' + self.member_id + '.json'
            file_utils.save_file(load_config.other_files_directory(), file_name, results)
            data_file = load_config.other_files_directory() + '/' + file_name

            self.files_to_process.append(data_file)

    def process(self, data_source_file):
        file_name = os.path.basename(data_source_file)

        load_config = self.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
        # load_config.process_count = 1

        data_processor = DataSourceProcessor(load_config, JSONDataSource(data_source_file))
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
                print('Usage: crossref_load_manager -member <member_id>')     
        arg_index += 1

if __name__ == '__main__':
    run()
