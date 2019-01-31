from data_load.base.load_manager import LoadManager

import data_load.irdb.add_funded_flag as add_funded_flag
import data_load.irdb.add_initial_grant_flag as add_initial_grant_flag

import data_load.irdb.spires_pub_projects.clear_relations as clear_spires_pubmed_relations
import data_load.irdb.spires_pub_projects.process_relationships as process_spires_relations
import data_load.irdb.spires_pub_projects.load_irdb_relations as load_spires_irdb_relations
import data_load.irdb.spires_pub_projects.load_pubmed_relations as load_spires_pubmed_relations

import data_load.irdb.extended_relations.create_extended_relations as create_extended_relations
import data_load.irdb.extended_relations.clear_relations as clear_extended_relations
import data_load.irdb.extended_relations.load_extended_relations as load_extended_relations

import data_load.irdb.patent_relations.generate_grant_numbers as generate_grant_numbers
import data_load.irdb.patent_relations.clear_relations as clear_derwent_relations
import data_load.irdb.patent_relations.load_derwent_relations as load_derwent_relations


import data_load.base.utils.file_utils as file_utils
import sys
import datetime
import psutil
import os

from shutil import copyfile
import data_load.irdb.irdb_load_config as irdb_load_config

from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_csv import CSVDataSource

from data_load.irdb.config import *
from data_load.base.load_config import LoadConfig
from data_load.base.constants import LOCAL_SERVER, ID_IRDB
from data_load.irdb.irdb_data_mapper import IRDBDataMapper
from data_load.irdb.irdb_data_extractor import IRDBDataExtractor


from data_load.base.utils.copy_tags_and_annotations import CopyTagsAndAnnotations

class IRDBLoadManager(LoadManager):

    def __init__(self, src_files_directory=None):
        super(IRDBLoadManager, self).__init__(ID_IRDB)
        self.src_data_directory = src_files_directory
        self.filtered_data_sources = None

  # Methods to override
    def should_reload(self):
        return False

    def mapping_file_path(self):
        return 'data_load/irdb/mapping.json'

    # def get_info_for_index_id(self, index_id):
    #     return {
    #         "index": "irdb_v4",
    #         "index_type": "grant"
    #     }

    def get_data_mapper(self):
        return IRDBDataMapper()

    def get_data_extractor(self):
        return IRDBDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []

        # tasks_list.append({
        #     'name': 'verify_data_sources',
        #     'status': ''
        # })
        for data_source in DATA_SOURCE_PROCESSING_ORDER:
            tasks_list.append({
                'name': data_source,
                'status': ''
            })
        tasks_list.append({
            'name': 'initial_grant_flag',
            'status': ''
        })
        tasks_list.append({
                'name': 'spires_relations',
                'sub_tasks': [
                    {
                        'name': 'process_spires_relations',
                        'status': ''
                    },
                    {
                        'name': 'load_spires_irdb_relations',
                        'status': ''
                    }
                ],
                'status': ''
            })
        # tasks_list.append({
        #         'name': 'extended_relations',
        #         'sub_tasks': [
        #             {
        #                 'name': 'create_extended_relations',
        #                 'status': ''
        #             },
        #             {
        #                 'name': 'clear_extended_relations',
        #                 'status': ''
        #             },
        #             {
        #                 'name': 'load_extended_relations',
        #                 'status': ''
        #             }
        #         ]
        #     })
        # tasks_list.append({
        #         'name': 'patent_relations',
        #         'sub_tasks': [
        #             {
        #                 'name': 'create_patent_relations',
        #                 'status': ''
        #             },
        #             {
        #                 'name': 'clear_patent_relations',
        #                 'status': ''
        #             },
        #             {
        #                 'name': 'load_patent_relations',
        #                 'status': ''
        #             }
        #         ]
        #     })
        tasks_list.append({
                'name': 'copy_tags_and_annotations',
                'status': ''
            })

        # Clear and load pubmed relations
        # tasks_list.append({
        #         'name': 'clear_pubmed_relations',
        #         'status': ''
        #     })
        # tasks_list.append({
        #         'name': 'load_spires_pubmed_relations',
        #         'status': ''
        #     })

        return tasks_list

    def run_task(self, task):
        load_config = self.get_load_config()
        irdb_load_config.reload_load_config = load_config

        task_name = task
        if task_name in  DATA_SOURCE_PROCESSING_ORDER:
            self.load_data_source(task_name)
        elif task_name == 'initial_grant_flag':
            add_initial_grant_flag.run(load_config)
        elif task_name == 'spires_relations':
            pass
        elif task_name == 'process_spires_relations':
            process_spires_relations.run()
        elif task_name == 'load_spires_irdb_relations':
            load_spires_irdb_relations.run()
        elif task_name == 'extended_relations':
            pass
        elif task_name == 'create_extended_relations':
            create_extended_relations.run()
        elif task_name == 'clear_extended_relations':
            clear_extended_relations.run()
        elif task_name == 'load_extended_relations':
            load_extended_relations.run()
        elif task_name == 'patent_relations':
            pass
        elif task_name == 'create_patent_relations':
            generate_grant_numbers.run() 
        elif task_name == 'clear_patent_relations':
            clear_derwent_relations.run()
        elif task_name == 'load_patent_relations':
            load_derwent_relations.run()
        elif task_name == 'copy_tags_and_annotations':
            self.copy_tags_and_annotations()

        elif task_name == 'clear_pubmed_relations':
            self.clear_pubmed_relations()
        elif task_name == 'load_spires_pubmed_relations':
            self.load_pubmed_relations()

    # def should_download_data(self):
    #     return True

    def download_data(self):
        load_config = self.get_load_config()
        source_files_directory = load_config.source_files_directory()
        for name in os.listdir(self.src_data_directory):
            file_path = os.path.join(self.src_data_directory, name)
            if os.path.isfile(file_path):
                dst_file_path = os.path.join(source_files_directory, name.lower()) 
                print 'Copying', file_path, '---->', dst_file_path
                copyfile(file_path, dst_file_path)

    def load_data_source(self, data_source_name):
        load_config = self.get_load_config()
        if self.filtered_data_sources is None:
            self.filtered_data_sources = self.verify_data_sources()

        # process data sources in the correct order
        if data_source_name in self.filtered_data_sources:
            file_name = self.filtered_data_sources[data_source_name]
            self.process_file(load_config, data_source_name, file_name)

    def process_file(self, load_config, data_source_name, file_name):
        # data_source_name = 'irdb'
        print 'processing', data_source_name

        # load_config = irdb_load_config.get_load_config()
        load_config.data_source_name = data_source_name

        source_files_directory = load_config.source_files_directory()
        data_source_file_path = source_files_directory + '/' + file_name

        data_processor = DataSourceProcessor(load_config, CSVDataSource(data_source_file_path))
        data_processor.run()

    def file_exists(self, load_config, file_name):
        source_files_directory = load_config.source_files_directory()
        file_name_comps = file_name.split('.')
        file_name_alt = file_name_comps[0].upper() + '.' + file_name_comps[1]

        data_source_file_path = source_files_directory + '/' + file_name
        data_source_file_path_alt = source_files_directory + '/' + file_name_alt

        # print data_source_file_path
        # print data_source_file_path_alt

        if (os.path.isfile(data_source_file_path) and os.path.exists(data_source_file_path)) or (os.path.isfile(data_source_file_path_alt) and os.path.exists(data_source_file_path_alt)):
            return True

        return False

    def verify_data_sources(self):
        load_config = self.get_load_config()
        source_files_directory = load_config.source_files_directory()
        print('Verifying data sources in ' + source_files_directory)

        missing_data_sources = {}
        for data_source in DATA_SOURCE_FILES:
            file_name = DATA_SOURCE_FILES[data_source]
            
            if not self.file_exists(load_config, file_name):
                missing_data_sources[data_source] = file_name

        if len(missing_data_sources) == 0:
            return DATA_SOURCE_FILES
        else:
            print('Missing data sources')
            print('--------------------')
            for data_source in missing_data_sources:
                file_name = missing_data_sources[data_source]

                print('* ' + str(file_name))

            filtered_data_sources = {}
            proceed = raw_input('Continue?')
            if proceed.lower() in ['y', 'yes', '']:
                for data_source in DATA_SOURCE_FILES:
                    if data_source not in missing_data_sources:
                        filtered_data_sources[data_source] = DATA_SOURCE_FILES[data_source]

            return filtered_data_sources    

    def clear_pubmed_relations(self):
        proceed = raw_input('Clear existing relationships from PubMed to IRDB? (y/n): ')
        if proceed.lower() in ['y', 'yes', '']:
            clear_spires_pubmed_relations.run()
        else:
            exit()

    def load_pubmed_relations(self):
        proceed = raw_input('Load new relationships from PubMed to IRDB? (y/n): ')
        if proceed.lower() in ['y', 'yes', '']:
            load_spires_pubmed_relations.run()
        else:
            exit()

def start(src_files_directory):
    irdb_reload = IRDBLoadManager(src_files_directory)
    # irdb_reload.del_config()
    irdb_reload.run()

def resume():
    irdb_reload = IRDBLoadManager()
    irdb_reload.run()

def analyse():
    irdb_reload = IRDBLoadManager()
    irdb_reload.analyse_failed_docs()

def run():
    src_files_directory = None
    index = None
    arg_index = 0
    for arg in sys.argv:
        if arg_index > 0:
            if arg == '-start':
                if (arg_index + 1) < len(sys.argv):
                    src_files_directory = sys.argv[arg_index + 1]
                    print('Source files directory: ' + str(src_files_directory))
                    start(src_files_directory=src_files_directory)
                else:
                    print('Usage: irdb_load_manager -start <src_files_directory>')
                return

            elif arg == '-resume':
                resume()
                return
            elif arg == '-analyse':
                analyse()
                return

        arg_index += 1
    
    print('Usage: irdb_load_manager -start <src_files_directory>')
    print('Usage: irdb_load_manager -resume')


if __name__ == "__main__":
    run()
