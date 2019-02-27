from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_xml import XMLDataSource
from data_load.base.load_manager import LoadManager

import data_load.base.utils.file_utils as file_utils
import data_load.base.utils.log_utils as log_utils
from data_load.base.utils.log_utils import *
from data_load.base.utils.export_doc_ids import get_doc_ids
from data_load.base.utils.copy_tags_and_annotations import CopyTagsAndAnnotations
from data_load.DATA_LOAD_CONFIG import PROCESS_COUNT

from data_load.base.constants import DATA_LOADING_DIRECTORY, TASK_STATUS_NOT_STARTED, ID_PUBMED

# from config import *
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper

from data_load.pubmed.email_client import EmailClient
from data_load.pubmed.ftp_manager import FTPManager
from data_load.pubmed.prospective_citations import FindProspectiveCitations
from data_load.pubmed.pubmed_updater import PubmedUpdater
from data_load.pubmed.pubmed_updater import DIR_PROSPECTS

import data_load.pubmed.file_manager as file_manager

from data_load.pubmed.clear_pubmed_relations import ClearPubmedRelations

import os
import sys
import time
import json

TASK_NAME = 'load_pubmed2018'


MODE_FILE = 'MODE_FILE'
MODE_BASELINE = 'MODE_BASELINE'
MODE_UPDATE = 'MODE_UPDATE'
MODE_COPY_USER_DATA = 'MODE_COPY_USER_DATA'
MODE_CLEAR_RELATIONS = 'MODE_CLEAR_RELATIONS'

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
        if self.mode == MODE_UPDATE:
            return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + 'pubmed2019_updates'
        elif self.mode == MODE_BASELINE:
            return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + 'pubmed2019'
        elif self.mode == MODE_COPY_USER_DATA:
            return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + 'copy_user_data'
        elif self.mode == MODE_CLEAR_RELATIONS:
            return DATA_LOADING_DIRECTORY + '/' + self.index_id.lower() + '/' + 'clear_relations'

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
                'name': 'save_update_record',
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

        elif self.mode == MODE_COPY_USER_DATA:
            tasks_list.append({
                'name': 'copy_user_data',
                'status': ''
            })
        elif self.mode == MODE_CLEAR_RELATIONS:
            tasks_list.append({
                'name': 'clear_rels',
                'status': ''
            })
        
        return tasks_list

    def run_task(self, task):
        if task == 'copy_user_data':
            self.copy_user_data()
        elif '_relations' in task:
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
            # pass
        elif task == 'send_update_notifications':
            self.send_update_notifications()
            # pass
        elif task == 'save_new_pmids':
            self.save_new_pmids()
        elif task == 'clear_rels':
            self.clear_relations()
     
    def clear_relations(self):
        load_config =self.get_load_config()
        clear_pubmed_relations = ClearPubmedRelations(load_config)
        clear_pubmed_relations.run()

    def tasks_completed(self):
        self.delete_task_list()

    def copy_user_data(self):
        load_config = self.get_load_config()
        copier = CopyTagsAndAnnotations(load_config.other_files_directory(), 
                                        load_config.server, 
                                        'pubmed2018_v5',
                                        'article',
                                        load_config.server,
                                        load_config.index,
                                        load_config.type)

        copier.run()

    def save_update_record(self):
        update_records_name = 'pubmed_update_records.json'

        update_records = file_utils.load_file(self.pubmed_updater.get_update_records_directory(), update_records_name)
        if len(update_records) == 0:
            update_records = []

        # update_records_for_date = []
        # if self.local_date_time in update_records:
        #     update_records_for_date = update_records[self.local_date_time]

        # update_records_for_date.extend(self.files_to_process)
        # update_records[self.local_date_time] = update_records_for_date
        
        update_data = self.pubmed_updater.generate_update_summary(self.files_to_process)

        update_file_records = []
        for update_file_path in update_data:
            update_file_name = os.path.basename(update_file_path)
            update_data_for_file = update_data[update_file_path]
            
            articles_processed = len(update_data_for_file['articles_processed'])
            new_articles = len(update_data_for_file['new_articles'])
            updated_articles = articles_processed - new_articles
            
            update_file_record_item = {
                'file_name': update_file_name,
                'file_path': update_file_path,
                'total_articles': articles_processed,
                'new_articles': new_articles,
                'updated_articles': updated_articles
            }

            update_file_records.append(update_file_record_item)

        update_record_item = {
            'date': self.local_date_time,
            'update_files': update_file_records 
        }

        update_records.append(update_record_item)

        file_utils.save_file(self.pubmed_updater.get_update_records_directory(), update_records_name, update_records)   

        # Save processed files list
        file_manager.update_processed_files(self.get_load_config(), self.files_to_process)   

    def load_update_records(self): 
        self.get_config()

        update_records_name = 'pubmed_update_records.json'
        update_records_directory = self.pubmed_updater.get_update_records_directory()

        print 'Loading update records from:', update_records_directory
        update_records = file_utils.load_file(update_records_directory, update_records_name)
        
        if len(update_records) == 0:
            print('0 update records, exiting')
            return

        print('Update records')
        print('--------------')
        index = 0
        for update_record in update_records:
            index += 1
            date = update_record['date']
            print(str(index) + ': ' + date)
        
        record_index = raw_input('Choose a record to retry' + '(' + str(1) + '-' + str(index) + ') ')
        try:
            record_index = int(record_index)
            if record_index >= 1 and record_index <= index:
                update_record = update_records[record_index - 1]
                date = update_record['date']
                process = raw_input('Process update record for ' + date + '? (y/n)')
                if process.lower() in ['y', 'yes']:
                    self.process_update_record(update_record)
            else:
                print('Wrong index, try again')
        except Exception as e:
            print(str(e))

    def process_update_record(self, update_record):
        date = update_record['date']
        update_files = update_record['update_files']

        print 'Processing update:', date
        print json.dumps(update_files, indent=4, sort_keys=True)

        files_to_process = []
        for update_file in update_files:
            files_to_process.append(update_file['file_path'])

        self.local_date_time = date
        self.files_to_process = files_to_process

        # self.del_config()
        # self.get_config()

        # self.find_prospective_citations()
        self.send_update_notifications()
    
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
        prospects_file_name = 'prospects_' + prospects_file_name + '.json'

        return prospects_file_name

    def save_prospects(self, prospects):
        update_record = {}
        update_record['update_files'] = self.files_to_process
        update_record['prospects'] = prospects
        update_record['date'] = self.local_date_time

        update_records_directory = self.pubmed_updater.get_update_records_directory(DIR_PROSPECTS)
        prospects_file_name = self.get_prospects_file_name()

        file_utils.save_file(update_records_directory, prospects_file_name, update_record) 

    def load_prospects(self):
        prospects_file_name = self.get_prospects_file_name()
        update_records_directory = self.pubmed_updater.get_update_records_directory(DIR_PROSPECTS)
        
        self.get_logger().info('Loading prospects...' + update_records_directory +  ' ' + prospects_file_name)

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

        if all_prospects is not None:
            self.get_logger().info(str(len(all_prospects)) + ' prospects loaded' )

        if all_prospects is None:
            self.get_logger().info('Prospects missing, finding again...')
            find_prospective_citations = FindProspectiveCitations(self.get_logger(), docs_with_new_citations)
            all_prospects = find_prospective_citations.run()
            self.save_prospects(all_prospects)

        self.get_logger().info('Sending prospective notifications...')
        self.send_notifications(all_prospects)
        
        self.get_logger().info('Sending update status mail...')
        EmailClient.send_update_notifications(self.local_date_time, update_data, all_prospects)

    def send_notifications(self, prospects):
        load_config = self.get_load_config()
        email_client = EmailClient(load_config)
        # print 'Prospects', all_prospects
        # self.get_logger().info('Prospects ' + str(prospects))
        failed_prospects = []

        local_date = self.local_date_time.split(' ')[0]

        # Send email notifications
        for prospect in prospects:
            problems = email_client.send_notification_for_prospect(prospect, local_date)
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

    def should_download_data(self):
        return True

    def download_data(self):
        load_config = self.get_load_config()
        ftp_manager = FTPManager(load_config)
        print 'Downloading data...'

        print 'Mode:', self.mode

        if self.mode == MODE_BASELINE:
            baseline_file_urls = ftp_manager.get_baseline_file_urls()
            print 'baseline_file_urls', len(baseline_file_urls)
            ftp_manager.download_missing_files(file_urls=baseline_file_urls, no_of_files=2)

            self.files_to_process = file_manager.get_baseline_files(load_config, baseline_file_urls)
            print 'files_to_process', len(self.files_to_process)

        elif self.mode == MODE_UPDATE:
            update_file_urls = ftp_manager.get_update_file_urls()
            files_to_process = file_manager.get_new_update_files(load_config, update_file_urls, self.no_of_files)
            files_to_download = self.no_of_files - len(files_to_process)

            # update_file_count = min(len(update_file_urls), self.no_of_files)
            # update_file_urls = update_file_urls[:update_file_count]
            # print 'Files to download', update_file_urls
            ftp_manager.download_missing_files(file_urls=update_file_urls, no_of_files=files_to_download)

            self.files_to_process = file_manager.get_new_update_files(load_config, update_file_urls, self.no_of_files)
            print 'Update', self.files_to_process
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
    load_manager.no_of_files = 1
    load_manager.del_config()
    load_manager.run()

def copy_user_data():
    load_manager = PubmedLoadManager(MODE_COPY_USER_DATA, 0)
    load_manager.del_config()
    load_manager.run()

def clear_relations():
    load_manager = PubmedLoadManager(MODE_CLEAR_RELATIONS, 0)
    load_manager.del_config()
    load_manager.run()

def retry_update_record():
    load_manager = PubmedLoadManager(MODE_UPDATE, 0)
    load_manager.del_config()
    load_manager.load_update_records()

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
            elif arg == '-copy':
                copy_user_data()
            elif arg == '-clear':
                clear_relations()
            elif arg == '-retry':
                retry_update_record()
            else: 
                print('Usage: pubmed_load_manager -n <number of files to process>')     
        arg_index += 1

    # resume()

if __name__ == '__main__':
    run()

