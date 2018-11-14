from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_xml import XMLDataSource
from data_load.base.load_manager import LoadManager

# from config import *
from data_load.pubmed2018.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2018.pubmed_data_mapper import PubmedDataMapper
from data_load.pubmed_update.ftp_manager import FTPManager
from data_load.base.constants import ID_PUBMED

import os
import sys
import time

import data_load.pubmed2018.file_manager as file_manager
from data_load.base.constants import DATA_LOADING_DIRECTORY, TASK_STATUS_NOT_STARTED

from data_load.base.utils.log_utils import *

TASK_NAME = 'load_pubmed2018'


class PubmedLoadManager(LoadManager):

    def __init__(self, no_of_files):
        super(PubmedLoadManager, self).__init__(ID_PUBMED)
        self.no_of_files = no_of_files
        self.files_to_process = []
        self.task_file_lookup = {}

    def get_root_directory(self, local_date):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + 'pubmed2018'

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "pubmed2018",
            "index_type": "article"
        }

    def mapping_file_path(self):
        return 'data_load/pubmed2018/mapping.json'

    def get_data_mapper(self):
        return PubmedDataMapper()

    def get_data_extractor(self):
        return PubmedDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []
        
        for pubmed_data_file in self.files_to_process:
            file_name = os.path.basename(pubmed_data_file)
            self.task_file_lookup[file_name] = pubmed_data_file

            tasks_list.append({
                'name': file_name,
                'status': ''
            })
            
        return tasks_list

    def run_task(self, task):
        self.process_file(task)
            
    def download_data(self):
        print 'Downloading data...'
        load_config = self.get_load_config()
        files_to_process = file_manager.get_new_files(load_config)
        files_to_download = self.no_of_files - len(files_to_process)

        ftp_manager = FTPManager(load_config)
        if files_to_download > 0:
            ftp_manager.download_n_files(files_to_download)
        
        self.files_to_process = file_manager.get_new_files(load_config)

    def process_file(self, file_name):
        pubmed_data_file = self.task_file_lookup[file_name]

        load_config = self.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
        load_config.log_level = LOG_LEVEL_DEBUG
        load_config.process_count = 1
        # load_config.set_logger(self.logger)

        # self.logger.info('Processing file ' + str(update_file))
        # self.logger.info('Processing docs... ' + str(file_name))

        data_processor = DataSourceProcessor(load_config, XMLDataSource(pubmed_data_file, 2))
        data_processor.run()

        file_manager.update_processed_files(load_config, [pubmed_data_file])

        # Process relationships
        # self.process_relationships(update_file, data_source_summary)

def start(no_of_files):
    load_manager = PubmedLoadManager(no_of_files)
    load_manager.del_config()
    load_manager.run()

def resume():
    load_manager = PubmedLoadManager(no_of_files)
    load_manager.run()

def run():
    arg_index = 0
    for arg in sys.argv:
        if arg_index > 0:
            if arg == '-n':
                if (arg_index + 1) < len(sys.argv):
                    no_of_files = sys.argv[arg_index + 1]
                    # try:
                        # print no_of_files
                    no_of_files = int(no_of_files)
                    start(no_of_files)
                    # except:
                        # print('Usage: pubmed_load_manager -n <number of files to process>')     
                    return
                else:
                    print('Usage: pubmed_load_manager -n <number of files to process>')     
            else: 
                print('Usage: pubmed_load_manager -n <number of files to process>')     
        arg_index += 1

    resume()

if __name__ == '__main__':
    run()

