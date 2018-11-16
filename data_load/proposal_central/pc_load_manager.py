from data_load.base.load_manager import LoadManager
from data_load.base.constants import DATA_LOADING_DIRECTORY
from data_load.proposal_central.pc_data_mapper import PCDataMapper
from data_load.proposal_central.pc_data_extractor import PCDataExtractor

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_csv import CSVDataSource

import os
import sys

ID_PROPOSAL_CENTRAL = "PROPOSAL_CENTRAL"

class PCLoadManager(LoadManager):

    def __init__(self, data_source_file):
        super(PCLoadManager, self).__init__(ID_PROPOSAL_CENTRAL)
        self.data_source_file = data_source_file

    def get_root_directory(self, local_date):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() 

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "proposal_central",
            "index_type": "proposal"
        }

    def mapping_file_path(self):
        return 'data_load/proposal_central/mapping.json'

    def get_data_mapper(self):
        return PCDataMapper()

    def get_data_extractor(self):
        return PCDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []
        
        tasks_list.append({
            'name': 'pc_load',
            'status': ''
        })
            
        return tasks_list

    def run_task(self, task):
        self.process(task)
            
    def download_data(self):
        pass

    def process(self, task):
        file_name = os.path.basename(self.data_source_file)

        load_config = self.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
        # load_config.process_count = 1

        data_processor = DataSourceProcessor(load_config, CSVDataSource(self.data_source_file))
        data_processor.run()

def start(data_source_file):
    load_manager = PCLoadManager(data_source_file)
    load_manager.del_config()
    load_manager.run()

def run():
    arg_index = 0
    for arg in sys.argv:
        if arg_index > 0:
            if arg == '-path':
                if (arg_index + 1) < len(sys.argv):
                    data_source_file = sys.argv[arg_index + 1]
                    start(data_source_file)   
                    return
                else:
                    print('Usage: pc_load_manager -path <path to csv file>')     
            else: 
                print('Usage: pc_load_manager -path <path to csv file>')     
        arg_index += 1

if __name__ == '__main__':
    run()
