from data_load.base.load_manager import LoadManager
from data_load.base.constants import DATA_LOADING_DIRECTORY
from data_load.uspto.uspto_data_mapper import USPTODataMapper
from data_load.uspto.uspto_data_extractor import USPTODataExtractor

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.uspto.data_source_xml import XMLDataSource
from data_load.base.utils.data_loader_utils import DataLoaderUtils

from data_load.grants import file_manager

import os
import sys

from data_load.base.constants import ID_USPTO

# ID_GRANTS = "GRANTS"


MODE_FILE = 'MODE_FILE'
MODE_AUTO = 'MODE_AUTO'

class USPTOLoadManager(LoadManager):

    def __init__(self, file=None):
        super(USPTOLoadManager, self).__init__(ID_USPTO)
        self.files_to_process = []

    def get_root_directory(self, local_date):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() 

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "uspto",
            "index_type": "grant"
        }

    def mapping_file_path(self):
        return 'data_load/uspto/mapping.json'

    def get_data_mapper(self):
        return USPTODataMapper()

    def get_data_extractor(self):
        return USPTODataExtractor()

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

    def check_and_create_index(self):
        data_loader_utils = DataLoaderUtils(self.server, self.index, self.type, self.server_username, self.server_password)
        mapping_file_path = self.mapping_file_path()
        print 'Checking index...'
        if not data_loader_utils.index_exists() and mapping_file_path is not None:
            mapping = data_loader_utils.load_mapping_from_file(mapping_file_path)
            data_loader_utils.create_index_from_mapping(mapping)

    def download_data(self):
        data_directory = '/Users/robin/Desktop/uspto'
        for name in os.listdir(data_directory):
            file_path = os.path.join(data_directory, name)
            if os.path.isfile(file_path) and name.endswith('.xml'):
                print 'Parsing file:', file_path
                self.files_to_process.append(file_path)

    def process(self, data_source_file):
        file_name = os.path.basename(data_source_file)

        load_config = self.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
        # load_config.process_count = 1

        data_processor = DataSourceProcessor(load_config, XMLDataSource(data_source_file, 2))
        data_processor.run()

def process_file(data_source_file):
    load_manager = USPTOLoadManager(MODE_FILE)
    load_manager.files_to_process.append(data_source_file)
    load_manager.del_config()
    load_manager.run()

def process_auto():
    load_manager = USPTOLoadManager(MODE_AUTO)
    load_manager.del_config()
    load_manager.run()

def run():
    arg_index = 0
    for arg in sys.argv:
        if arg_index > 0:
            if arg == '-path':
                if (arg_index + 1) < len(sys.argv):
                    data_source_file = sys.argv[arg_index + 1]
                    process_file(data_source_file)   
                    return
                else:
                    print('Usage: uspto_load_manager -path <path to csv file>')     
            elif arg == '-auto': 
                process_auto()
            else:
                print('Usage: uspto_load_manager -path <path to csv file>')     
        arg_index += 1

if __name__ == '__main__':
    run()
