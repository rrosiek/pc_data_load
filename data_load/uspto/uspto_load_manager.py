from data_load.base.load_manager import LoadManager
from data_load.base.constants import DATA_LOADING_DIRECTORY
from data_load.uspto.uspto_data_mapper import USPTODataMapper
from data_load.uspto.uspto_data_extractor import USPTODataExtractor

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.uspto.data_source_xml import XMLDataSource
from data_load.base.utils.data_loader_utils import DataLoaderUtils
import data_load.base.utils.file_utils as file_utils

from data_load.uspto import file_manager

import get_data_source_links

import os
import sys
import json

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
    
    def should_download_data(self):
        return True

    def tasks_completed(self):
        self.delete_task_list()
        load_config = self.get_load_config()
        file_manager.set_processed_files(load_config, self.files_to_process)

    def download_data(self):
        # data_directory = '/Users/robin/Desktop/uspto'
        # for name in os.listdir(data_directory):
        #     file_path = os.path.join(data_directory, name)
        #     if os.path.isfile(file_path) and name.endswith('.xml'):
        #         print 'Parsing file:', file_path
        #         self.files_to_process.append(file_path)
        load_config = self.get_load_config()

        self.files_to_process = file_manager.get_files_to_process(load_config)
        # self.files_to_process = file_manager.download_files(load_config, '2019')
        print self.files_to_process

    def process(self, data_source_file):
        file_name = os.path.basename(data_source_file)

        load_config = self.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
        # load_config.process_count = 1

        data_processor = DataSourceProcessor(load_config, XMLDataSource(data_source_file, 2))
        data_processor.run()

    def analyse_failed_docs(self):
        self.get_config()

        print 'Analysing failed docs'
        load_config = self.get_load_config()
        failed_docs_files = load_config.get_failed_docs_files()
        print len(failed_docs_files), 'failed doc files'
        for failed_docs_file in failed_docs_files:
            print 'Loading file:', failed_docs_file
            failed_docs = file_utils.load_file_path(failed_docs_file)
            for failed_doc in failed_docs:
                reason = failed_docs[failed_doc]['reason']
                doc = failed_docs[failed_doc]['doc']
                print failed_doc
                error_reason = None
                if isinstance(reason, dict):
                    if 'index' in reason:
                        index = reason['index']
                    
                    elif 'update' in reason:
                        index = reason['update']

                    if 'error' in index:
                        error = index['error']
                        if 'reason' in error:
                            error_reason = error['reason']
                            
                if error_reason is None or len(error_reason) == 0:
                    print failed_docs[failed_doc]['reason']
                else:
                    print 'Reason:', error_reason

                print_doc = raw_input('Print doc?')
                if print_doc.lower() in ['y', 'yes']:
                    print json.dumps(failed_docs[failed_doc]['reason'])

                

def process_file(data_source_file):
    load_manager = USPTOLoadManager(MODE_FILE)
    load_manager.files_to_process.append(data_source_file)
    load_manager.del_config()
    load_manager.run()

def process_auto():
    load_manager = USPTOLoadManager(MODE_AUTO)
    load_manager.del_config()
    load_manager.run()


def analyse():
    load_manager = USPTOLoadManager(MODE_AUTO)
    load_manager.analyse_failed_docs()


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
            elif arg == '-analyse': 
                analyse()
            else:
                print('Usage: uspto_load_manager -path <path to csv file>')     
        arg_index += 1

if __name__ == '__main__':
    run()
