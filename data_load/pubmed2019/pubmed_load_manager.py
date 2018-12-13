from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_xml import XMLDataSource
from data_load.base.load_manager import LoadManager

import data_load.base.utils.file_utils as file_utils
import data_load.base.utils.log_utils as log_utils
from data_load.base.utils.log_utils import *
from data_load.base.utils.export_doc_ids import get_doc_ids
from data_load.DATA_LOAD_CONFIG import PROCESS_COUNT

from data_load.base.constants import DATA_LOADING_DIRECTORY, TASK_STATUS_NOT_STARTED, ID_PUBMED

# from config import *
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper

from data_load.pubmed import email_client
from data_load.pubmed.ftp_manager import FTPManager
from data_load.pubmed.prospective_citations import FindProspectiveCitations
from data_load.pubmed.pubmed_updater import PubmedUpdater

import data_load.pubmed.file_manager as file_manager

import os
import sys
import time

TASK_NAME = 'load_pubmed2018'


MODE_FILE = 'MODE_FILE'
MODE_BASELINE = 'MODE_BASELINE'
MODE_UPDATE = 'MODE_UPDATE'

ID_PUBMED_2019 = 'PUBMED_2019'

class PubmedLoadManager(LoadManager):

    def __init__(self, mode=MODE_FILE, no_of_files=0):
        super(PubmedLoadManager, self).__init__(ID_PUBMED_2019)
        self.mode = mode
        self.no_of_files = no_of_files
        self.files_to_process = []
        self.file_path_lookup = {}

        self.logger = None

        self.pubmed_updater = PubmedUpdater(self)

    def get_logger(self):
        if self.logger is None:
            self.logger = log_utils.create_logger('pubmed2019_update', self.get_load_config().log_files_directory())

        return self.logger

    def get_root_directory(self, local_date):
        return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + 'pubmed2019'

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "pubmed2019",
            "index_type": "article"
        }

    def mapping_file_path(self):
        return 'data_load/pubmed2019/mapping.json'

    def get_data_mapper(self):
        return PubmedDataMapper()

    def get_data_extractor(self):
        return PubmedDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []

        if self.mode == MODE_UPDATE:
            tasks_list.append({
                'name': 'get_existing_pmids',
                'status': ''
            })
        
            for pubmed_data_file in self.files_to_process:
                file_name = os.path.basename(pubmed_data_file)
                self.file_path_lookup[file_name] = pubmed_data_file

                tasks_list.append({
                    'name': file_name + '_' + 'data',
                    'status': ''
                })

                tasks_list.append({
                    'name': file_name + '_' + 'relations',
                    'status': ''
                })

            tasks_list.append({
                'name': 'find_prospective_citations',
                'status': ''
            })

            tasks_list.append({
                'name': 'send_update_notifications',
                'status': ''
            })

            tasks_list.append({
                'name': 'save_new_pmids',
                'status': ''
            })

            tasks_list.append({
                'name': 'update_processed_files',
                'status': ''
            })

        elif self.mode == MODE_BASELINE:
            for pubmed_data_file in self.files_to_process:
                file_name = os.path.basename(pubmed_data_file)
                self.file_path_lookup[file_name] = pubmed_data_file

                tasks_list.append({
                    'name': file_name + '_' + 'data',
                    'status': ''
                })

            for pubmed_data_file in self.files_to_process:
                file_name = os.path.basename(pubmed_data_file)
                self.file_path_lookup[file_name] = pubmed_data_file

                tasks_list.append({
                    'name': file_name + '_' + 'relations',
                    'status': ''
                })
        
        return tasks_list

    def run_task(self, task):
        if '_relations' in task:
            task = task.replace('_relations', '')
            pubmed_data_file = self.file_path_lookup[task]
            self.pubmed_updater.process_relationships(pubmed_data_file)
        elif '_data' in task:
            task = task.replace('_data', '')
            pubmed_data_file = self.file_path_lookup[task]
            self.pubmed_updater.process_file(pubmed_data_file)
        elif task == 'save_update_record':
            self.save_update_record()
        elif task == 'get_existing_pmids':
            self.pubmed_updater.get_existing_pmids()
        elif task == 'find_prospective_citations':
            self.find_prospective_citations()
        elif task == 'send_update_notifications':
            self.send_update_notifications()
        elif task == 'save_new_pmids':
            self.save_new_pmids()

    def save_update_record(self):
        update_records_name = 'pubmed_update_records.json'

        update_records = file_utils.load_file(self.pubmed_updater.get_update_records_directory(), update_records_name)
        
        update_records_for_date = []
        if self.local_date_time in update_records:
            update_records_for_date = update_records[self.local_date_time]

        update_records_for_date.extend(self.files_to_process)
        update_records[self.local_date_time] = update_records_for_date

        file_utils.save_file(self.pubmed_updater.get_update_records_directory(), update_records_name, update_records)   
        file_manager.update_processed_files(self.get_load_config(), [self.files_to_process])    
    
    def find_prospective_citations(self):
        docs_with_new_citations = self.pubmed_updater.get_docs_with_new_citations(self.files_to_process)
        print 'Find prospective citations', len(docs_with_new_citations), 'docs with new citations'

        # Find prospects
        find_prospective_citations = FindProspectiveCitations(self.get_logger(), docs_with_new_citations)
        all_prospects = find_prospective_citations.run()

        # Save prospects
        self.save_prospects(all_prospects)

    def get_prospects_file_name(self):
        prospects_file_name = self.local_date_time.replace(' ', '_')
        prospects_file_name = prospects_file_name.replace(':', '_')
        prospects_file_name = 'prospects_' + prospects_file_name

        return prospects_file_name

    def save_prospects(self, prospects):
        update_record = {}
        update_record['update_files'] = self.files_to_process
        update_record['prospects'] = prospects
        update_record['date'] = self.local_date_time

        update_records_directory = self.pubmed_updater.get_update_records_directory()
        prospects_file_name = self.get_prospects_file_name()

        file_utils.save_file(update_records_directory, prospects_file_name, update_record) 

    def load_prospects(self):
        prospects_file_name = self.get_prospects_file_name()
        update_records_directory = self.pubmed_updater.get_update_records_directory()

        update_record = file_utils.load_file(update_records_directory, prospects_file_name)
        if 'prospects' in update_record:
            return update_record['prospects']
        else:
            return None

    def send_update_notifications(self):
        # Get update summary and docs with new citations
        update_data = self.pubmed_updater.generate_update_summary(self.files_to_process)
        docs_with_new_citations =  self.pubmed_updater.get_docs_with_new_citations(self.files_to_process)
        all_prospects = self.load_prospects()

        if all_prospects is None:
            find_prospective_citations = FindProspectiveCitations(self.get_logger(), docs_with_new_citations)
            all_prospects = find_prospective_citations.run()

        self.get_logger().info('Sending prospective notifications...')
        self.send_notifications(all_prospects)
        
        self.get_logger().info('Sending update status mail...')
        email_client.send_update_notifications(self.local_date_time, update_data, all_prospects)

    def send_notifications(self, prospects):
        # print 'Prospects', all_prospects
        self.get_logger().info('Prospects ' + str(prospects))
        failed_prospects = []

        # Send email notifications
        for prospect in prospects:
            problems = email_client.send_notification_for_prospect(prospect)
            if len(problems) > 0:
                failed_prospects.append({
                    'problems': problems,
                    'prospect': prospect
                })

        # Dump failed prospects to file
        if len(failed_prospects) > 0:
            file_utils.save_file(self.get_load_config().other_files_directory(), 'failed_prospects.json', failed_prospects)

    def save_new_pmids(self):
        self.pubmed_updater.save_new_pmids(self.files_to_process)

    def download_data(self):
        load_config = self.get_load_config()
        ftp_manager = FTPManager(load_config)
        print 'Downloading data...'

        print 'Mode:', self.mode

        if self.mode == MODE_BASELINE:
            baseline_file_urls = ftp_manager.get_baseline_file_urls()
            print 'baseline_file_urls', len(baseline_file_urls)
            ftp_manager.download_missing_files(baseline_file_urls)

            self.files_to_process = file_manager.get_baseline_files(load_config, baseline_file_urls)
            print 'files_to_process', len(self.files_to_process)

        elif self.mode == MODE_UPDATE:
            update_file_urls = ftp_manager.get_update_file_urls()
            update_file_count = min(len(update_file_urls), self.no_of_files)
            update_file_urls = update_file_urls[:update_file_count]
            ftp_manager.download_missing_files(update_file_urls)

            self.files_to_process = file_manager.get_new_update_files(load_config, update_file_urls, self.no_of_files)
        else:
            files_to_process = file_manager.get_new_files(load_config)
            files_to_download = self.no_of_files - len(files_to_process)

            if files_to_download > 0:
                ftp_manager.download_n_files(files_to_download)
            
            self.files_to_process = file_manager.get_new_files(load_config)

def start(no_of_files):
    load_manager = PubmedLoadManager(MODE_FILE, no_of_files)
    load_manager.mode = MODE_FILE
    load_manager.del_config()
    load_manager.run()

def process_baseline():
    load_manager = PubmedLoadManager(MODE_BASELINE, 0)
    load_manager.mode = MODE_BASELINE
    load_manager.del_config()
    load_manager.run()

def process_updates():
    load_manager = PubmedLoadManager(MODE_UPDATE, 0)
    load_manager.mode = MODE_UPDATE
    load_manager.no_of_files = 2
    # load_manager.del_config()
    load_manager.run()

def resume():
    load_manager = PubmedLoadManager(0)
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
            elif arg == '-baseline':
                process_baseline()
            elif arg == '-update':
                process_updates()
            else: 
                print('Usage: pubmed_load_manager -n <number of files to process>')     
        arg_index += 1

    resume()

if __name__ == '__main__':
    run()

