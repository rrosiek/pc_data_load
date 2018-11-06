from utils.data_loader_utils import DataLoaderUtils
import data_load.base.utils.file_utils as file_utils
from data_load.base.load_config import LoadConfig

from data_load.base.utils.copy_tags_and_annotations import CopyTagsAndAnnotations

import data_load.base.utils.es_utils as es_utils
import time

from data_load.base.constants import *
import psutil
import sys
import datetime
import psutil
import os


class LoadManager(object):

    def __init__(self, index_id):
        self.tasks_list = self.get_tasks_list()
        self.index_id = index_id
        self.server = LOCAL_SERVER
        self.index = None
        self.type = None
        self.root_directory = None
        self.src_data_directory = None
        self.src_data_exists = False
        self.config_file = self.index_id + '_CONFIG.json'

    def create_config(self):
        now = datetime.datetime.now()
        local_date = now.strftime("%m-%d-%Y_%H:%M")
        self.root_directory = self.get_root_directory()
        file_utils.make_directory(self.root_directory)

        index_item = es_utils.get_info_for_index_id(self.index_id)
        self.index = index_item['index']
        self.type = index_item['index_type']

        if self.should_reload():
            index_comps = self.index.split('_')
            index_version = ''
            if len(index_comps) > 1:
                index_version = index_comps[-1]
                index_comps = index_comps[:-1]
                if index_version.startswith('v'):
                    index_version = index_version.replace('v', '')
                    try:
                        index_version_number = int(index_version)
                        index_version_number += 1
                        index_version = str(index_version_number)
                    except Exception as e:
                        print e
                    index_version = 'v' + index_version
                else:
                    version = 'v2'
                    if len(index_version) > 0:
                        version = '_' + version
                    
                    index_version = index_version + version
            else:
                version = 'v2'
                if len(index_version) > 0:
                    version = '_' + version
                    
                index_version = index_version + version
   
            self.index = '_'.join(index_comps)

            if len(index_version) > 0:
                self.index += '_' + index_version

        print 'root directory:', self.root_directory
        print 'index_id:', self.index_id
        print 'server:', self.server
        print 'index:', self.index
        print 'type:', self.type

        config = self.set_config()
        return config

    def get_root_directory(self):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + self.index_id.lower() + '_' + local_date

    def del_config(self):
        file_utils.save_file(DATA_LOADING_DIRECTORY, self.config_file, {})

    def get_config(self):
        config = file_utils.load_file(DATA_LOADING_DIRECTORY, self.config_file)
        if len(config) == 0:
            config = self.create_config()

        self.root_directory = config['root_directory']
        self.index_id = config['index_id']
        self.server = config['server']
        self.index = config['index']
        self.type = config['type'] 
        self.src_data_exists = config['src_data_exists']
        if 'src_data_directory' in config:
            self.src_data_directory = config['src_data_directory']

        return config
  
    def set_config(self):
        config = {}
        config['root_directory'] = self.root_directory
        config['index_id'] = self.index_id
        config['server'] = self.server
        config['index'] = self.index
        config['type'] = self.type
        config['src_data_exists'] = self.src_data_exists
        config['src_data_directory'] = self.src_data_directory
        
        file_utils.save_file(DATA_LOADING_DIRECTORY, self.config_file, config)
        return config

    def get_load_config(self):
        load_config = LoadConfig()
        load_config.root_directory = self.root_directory
        load_config.process_count = psutil.cpu_count()

        load_config.server = self.server
        load_config.index = self.index
        load_config.type = self.type

        load_config.data_mapper = self.get_data_mapper()
        load_config.data_extractor = self.get_data_extractor()
        load_config.max_memory_percent = self.get_max_memory_percent()

        return load_config

    # Methods to override
    def should_reload(self):
        return False

    def mapping_file_path(self):
        return None

    def get_data_mapper(self):
        return None

    def get_data_extractor(self):
        return None

    def get_max_memory_percent(self):
        return 75

    def download_data(self):
        pass

    def get_tasks_list(self):
        return []

    def run_task(self, task_name):
        pass

    def run(self):
        # Get config
        self.get_config()

        # Create index
        print 'Creating index...'
        self.check_and_create_index()

        # Download data
        if not self.src_data_exists:
            print 'Downloading data...'

            self.download_data()
            self.src_data_exists = True
            self.set_config()

        tasks_list = self.create_tasks_list()
        self.check_and_start_tasks(tasks_list)

    def start_task(self, task):
        task_name = task['name']
        self.update_status_for_task(task_name, TASK_STATUS_IN_PROGRESS)
        self.run_task(task_name)
        self.update_status_for_task(task_name, TASK_STATUS_COMPLETED)

    def check_and_start_tasks(self, tasks_list):
        for task in tasks_list:
            status = self.get_status(task)

            if status == TASK_STATUS_NOT_STARTED or status == TASK_STATUS_IN_PROGRESS:
                # TODO start task
                sub_tasks = self.get_sub_tasks(task)
                if len(sub_tasks) == 0:
                    self.start_task(task)
                else:
                    self.check_and_start_tasks(sub_tasks)
            elif status == TASK_STATUS_COMPLETED:
                # TODO skip
                print task['name'], 'completed'

    def check_and_create_index(self):
        data_loader_utils = DataLoaderUtils(self.server, self.index, self.type)
        mapping_file_path = self.mapping_file_path()
        if not data_loader_utils.index_exists() and mapping_file_path is not None:
            data_loader_utils.create_index(mapping_file_path)

    def delete_index(self, _server, _index, _type):
        data_loader_utils = DataLoaderUtils(_server, _index, _type)
        data_loader_utils.delete_index()

    def create_index(self, _server, _index, _type, mapping_file_path):
        # Check and create index

        data_loader_utils = DataLoaderUtils(_server, _index, _type)
        if data_loader_utils.index_exists():
            # Prompt to delete & recreate index
            input = raw_input("Delete and recreate index, " + _index + "? (y/n)")
            if input in ['y', 'yes']:
                data_loader_utils.delete_index()
                print 'Waiting...'
                time.sleep(5)
                data_loader_utils.create_index(mapping_file_path)
        else:
            # Create Index
            data_loader_utils.create_index(mapping_file_path)


    # Tasks

    def update_status_for_task(self, task_name, status):
        task_list = self.load_tasks_list()
        self.update_status_from_task_list(task_name, task_list, status)
        self.save_tasks_list(task_list)
            
    def update_status_from_task_list(self, task_name, task_list, status):
        for task in task_list:
            if task_name == task['name']:
                task['status'] = status
            else:
                if 'sub_tasks' in task:
                    sub_tasks = task['sub_tasks']
                    self.update_status_from_task_list(task_name, sub_tasks, status)

    def create_tasks_list(self):
        tasks_list = self.load_tasks_list()
        if len(tasks_list) == 0:
            tasks_list = self.get_tasks_list()
            for task in tasks_list:
                self.set_status(task, TASK_STATUS_NOT_STARTED)

        self.save_tasks_list(tasks_list)
        return tasks_list

    def save_tasks_list(self, tasks_list):
        print 'Saving tasks list', self.root_directory
        file_utils.save_file(self.root_directory, 'tasks_list.json', tasks_list)

    def load_tasks_list(self):
        print 'Loading tasks list', self.root_directory
        tasks_list = file_utils.load_file(self.root_directory, 'tasks_list.json')
        return tasks_list

    def set_status(self, task, status):
        task['status'] = status
        if 'sub_tasks' in task:
            for sub_task in task['sub_tasks']:
                self.set_status(sub_task, status)   

    def get_status(self, task):
        if 'status' in task:
            return task['status']
        
        return TASK_STATUS_NOT_STARTED

    def get_sub_tasks(self, task):
        if 'sub_tasks' in task:
            return task['sub_tasks']
        
        return []

    def copy_tags_and_annotations(self):
        print 'Copying tags and annotations'
        load_config = self.get_load_config()
        server = load_config.server

        index_item = es_utils.get_info_for_index_id(self.index_id)
        src_index = index_item['index']
        src_type = index_item['index_type']

        reports_directory = load_config.data_source_directory('copy_tags_and_annotations')

        copier = CopyTagsAndAnnotations(reports_directory=reports_directory,
                                        src_server=server,
                                        src_index=src_index,
                                        src_type=src_type,
                                        dest_server=server,
                                        dest_index=load_config.index,
                                        dest_type=load_config.type)

        copier.run()    