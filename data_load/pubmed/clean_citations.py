
from data_load.base.constants import DATA_LOADING_DIRECTORY, ID_PUBMED
from data_load.base.utils.log_utils import LOG_LEVEL_WARNING
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig
from data_load.base.data_source_xml import XMLDataSource
from data_load.base.utils.data_loader_utils import DataLoaderUtils

from data_load.pubmed.ftp_manager import FTPManager
import data_load.pubmed.file_manager as file_manager
from data_load.base.utils import file_utils
from multiprocessing import Process

import psutil
import threading
import sys
import time
import os
import json
import datetime

ID_PUBMED_2019 = 'PUBMED_2019'

baseline_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'pubmed2019'
updates_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'pubmed2019_updates'
clean_citations_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'clean_citations'

SERVER = 'http://localhost:9200'
INDEX = 'pubmed2019'
TYPE = 'article'


class ProcessBaselineFile(object):

    def __init__(self, load_config, updated_docs, baseline_file):
        self.baseline_file = baseline_file
        self.updated_docs = updated_docs
        self.load_config = load_config

        file_name = os.path.basename(baseline_file)
        self.current_baseline_file = file_name.split('.')[0]

        self.original_docs = {}
        self.inverted_index = {}


    def extract_id(self, name, row, current_index):
        if self.load_config.data_extractor is not None:
            if self.load_config.data_extractor.should_generate_id(name):
                return self.load_config.data_extractor.generate_id(current_index)
            else:
                return self.load_config.data_extractor.extract_id(name, row)

        self.load_config.log(LOG_LEVEL_WARNING, 'Error: no data extractor configured')
        return None

    def extract_data(self, _id, name, row):
        if self.load_config.data_extractor is not None:
            return self.load_config.data_extractor.extract_data(_id, name, row)

        return row

    def run(self):
        xml_data_source = XMLDataSource(self.baseline_file, 2)
        xml_data_source.process_rows(self.process_baseline_row)

        print len(self.original_docs), self.current_baseline_file
        if len(self.original_docs) > 0:
            file_utils.save_file(self.load_config.generated_files_directory(), 'original_docs_' + self.current_baseline_file + '.json', self.original_docs)
       
        file_utils.save_file(self.load_config.generated_files_directory(), 'inverted_index_' + self.current_baseline_file + '.json', self.inverted_index)

    def process_baseline_row(self, row, current_index):
        if current_index % 10000 == 0:
            print self.current_baseline_file, current_index
        _id = self.extract_id(self.load_config.data_source_name, row, current_index)
        if _id is not None:
            self.inverted_index[_id] = self.current_baseline_file
            if  _id in self.updated_docs:
                doc = self.extract_data(_id, self.load_config.data_source_name, row)
                if doc is not None and len(doc) > 0:
                    self.original_docs[_id] = doc

                # if len(self.original_docs) % 100 == 0:
                # print 'Original docs', len(self.original_docs)

        return True


class CleanCitations(object):

    def __init__(self):
        self.updated_docs = {}
        self.original_docs = {}

        self.server = SERVER
        self.index = INDEX
        self.type = TYPE

        self.server_username = ''
        self.server_password = ''

        self.load_config = self.get_load_config(clean_citations_directory)
        self.data_loader_utils = DataLoaderUtils(self.load_config.server, self.load_config.index, self.load_config.type, self.load_config.server_username, self.load_config.server_password)

        self.docs_with_updates = {}

        self.inverted_index = {}
        self.current_baseline_file = None
        self.current_update_file = None

        self.processes = []
        self.missing_docs = {}

        self.inverted_index_for_updated_docs = {}

    def run(self):
        # self.get_updated_docs()
        self.updated_docs = file_utils.load_file(self.load_config.other_files_directory(), 'updated_docs.json')
        print 'Updated docs:', len(self.updated_docs)
        print 'Original docs:', len(self.original_docs)

        # self.get_original_docs()
        self.original_docs = file_utils.load_file(self.load_config.other_files_directory(), 'original_docs.json')
        self.inverted_index = file_utils.load_file(self.load_config.other_files_directory(), 'inverted_index.json')
        self.inverted_index_for_updated_docs = file_utils.load_file(self.load_config.other_files_directory(), 'inverted_index_for_updated_docs.json')

        print 'Updated docs:', len(self.updated_docs)
        print 'Original docs:', len(self.original_docs)
        print 'Inverted index:', len(self.inverted_index)
        print 'inverted_index_for_updated_docs:', len(self.inverted_index_for_updated_docs)
        print json.dumps(self.inverted_index_for_updated_docs)
        # input = raw_input('Continue?')
        # if input.lower() in ['n', 'no', '0']:
        #     sys.exit(1)

        self.update_docs()

        print 'Docs with updates', len(self.docs_with_updates)
        print json.dumps(self.docs_with_updates)

        print 'Missing docs'
        print json.dumps(self.missing_docs.keys())

    def update_docs(self):
        for _id in self.updated_docs:
            if _id in self.original_docs:
                original_doc = self.original_docs[_id]
                updated_doc = self.updated_docs[_id]
                # print original_doc
                # print updated_doc
                original_citations = self.load_config.data_mapper.get_citations([original_doc])
                updated_citations = self.load_config.data_mapper.get_citations([updated_doc])

                # print _id, 'original', len(original_citations), 'updated', len(updated_citations)
                if not self.compare_citations(original_citations, updated_citations):
                    self.docs_with_updates[_id] = {
                        'original_citations': len(original_citations),
                        'updated_citations': len(updated_citations),
                        'original_doc': original_doc,
                        'updated_doc': updated_doc
                    }

                    added_citations = []
                    removed_citations = []
                    for _id in updated_citations:
                        if _id not in original_citations:
                            added_citations.append(_id)

                    for _id in original_citations:
                        if _id not in updated_citations:
                            removed_citations.append(_id)

                    update_file = self.inverted_index_for_updated_docs[_id]
                    self.update_doc_with_history(_id, update_file, original_citations, removed_citations, added_citations)
                # self.update_doc(_id, original_citations)

            else:
                updated_doc = self.updated_docs[_id]
                self.missing_docs[_id] = updated_doc
                updated_citations = self.load_config.data_mapper.get_citations([updated_doc])

                print 'Missing doc', _id, updated_citations

    def compare_citations(self, original_citations, updated_citations):
        for _id in original_citations:
            if _id not in updated_citations:
                return False

        for _id in updated_citations:
            if _id not in original_citations:
                return False

        return True

    def get_existing_doc(self, _id):
        exisiting_doc = self.data_loader_utils.fetch_doc(_id)
        if exisiting_doc is not None and '_source' in exisiting_doc:
            exisiting_doc = exisiting_doc['_source']
        return exisiting_doc

    def update_doc_with_history(self, _id, update_file, original_citations, removed_citations, added_citations):
        print _id, update_file, 'original_citations', len(original_citations), 'removed_citations', len(removed_citations), 'added_citations', len(added_citations)
        now = datetime.datetime.now()

        updated_date = now.isoformat()

        existing_doc = self.get_existing_doc(_id)

        # update_file = os.path.basename(self.data_source.data_source_file_path)

        # Create the update history item
        update_history_item = {
            "updated_date": updated_date,
            "update_file": update_file,
            "removed_citations": removed_citations,
            "added_citations": added_citations
        }

        # Get the existing update history
        update_history = []
        if 'update_history' in existing_doc:
            update_history = existing_doc['update_history']

        # Add the original citations list if not present
        if len(update_history) == 0:
            update_history.append({
                "original_citations": original_citations
            })
            
        # Add the new update history item
        update_history.append(update_history_item)

        doc = {
            "update_history": update_history
        }
        
        doc = {
            'doc': doc
        }

        # self.data_loader_utils.update_doc(_id, doc)
        
    def update_doc(self, _id, original_citations):
        print 'Updating doc', _id, len(original_citations), 'citations'
        # input = raw_input('Continue?')
        # if input.lower() in ['n', 'no', '0']:
        #     sys.exit(1)

        # Get the existing update history
        update_history = []
    
        # Add the original citations list if not present
        if len(update_history) == 0:
            update_history.append({
                "original_citations": original_citations
            })

        doc = {
            "update_history": update_history
        }
        
        doc = {
            'doc': doc
        }

        self.data_loader_utils.update_doc(_id, doc)          


    def get_original_docs(self):
        load_config = self.get_load_config(baseline_directory)
        ftp_manager = FTPManager(load_config)

        baseline_file_urls = ftp_manager.get_baseline_file_urls()
        # ftp_manager.download_missing_files(file_urls=baseline_file_urls, no_of_files=10)
        baseline_files = file_manager.get_baseline_files(load_config, baseline_file_urls)

        print 'Baseline files:', len(baseline_files)

        for baseline_file in baseline_files:
            # self.process_baseline_file(baseline_file)
            process = Process(target=self.process_baseline_file, args=(baseline_file,))
            process.start()

            self.processes.append(process)
            if len(self.processes) >= 16:
                old_process = self.processes.pop(0)
                old_process.join()

            time.sleep(0.5)

        while len(self.processes) > 0:
            old_process = self.processes.pop(0)
            old_process.join()

        self.combine_inverted_index()
        self.combine_original_docs()

        # file_utils.save_file(self.load_config.other_files_directory(), 'original_docs.json', self.original_docs)
        # file_utils.save_file(self.load_config.other_files_directory(), 'inverted_index.json', self.inverted_index)

    def combine_inverted_index(self):
        files = []
        generated_files_directory = self.load_config.generated_files_directory()
        for name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, name)
            if os.path.isfile(file_path) and name.startswith('inverted_index_'):
                files.append(name)

        combined = {}
        for name in files:
            data = file_utils.load_file(generated_files_directory, name)
            combined.update(data)

        file_utils.save_file(self.load_config.other_files_directory(), 'inverted_index.json', combined)

    def combine_original_docs(self):
        files = []
        generated_files_directory = self.load_config.generated_files_directory()
        for name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, name)
            if os.path.isfile(file_path) and name.startswith('original_docs_'):
                files.append(name)
         
        combined = {}
        for name in files:
            data = file_utils.load_file(generated_files_directory, name)
            combined.update(data)

        file_utils.save_file(self.load_config.other_files_directory(), 'original_docs.json', combined)


    def process_baseline_file(self, baseline_file):
        print "Processing file:", baseline_file

        process_file = ProcessBaselineFile(self.load_config, dict.fromkeys(self.updated_docs.keys()), baseline_file)
        process_file.run()

    # def process_baseline_file(self, baseline_file):
    #     print "Processing file:", baseline_file

    #     file_name = os.path.basename(baseline_file)
    #     self.current_baseline_file = file_name.split('.')[0]

    #     last_time_stamp = time.time()        
        
    #     xml_data_source = XMLDataSource(baseline_file, 2)
    #     xml_data_source.process_rows(self.process_baseline_row)

    #     current_time_stamp = time.time()
    #     diff = current_time_stamp - last_time_stamp

    #     print 'Time for file', baseline_file, diff


    def process_baseline_row(self, row, current_index):
        if current_index % 100 == 0:
            print current_index
        _id = self.extract_id(self.load_config.data_source_name, row, current_index)
        if _id is not None:
            self.inverted_index[_id] = self.current_baseline_file
            if  _id in self.updated_docs:
                doc = self.extract_data(_id, self.load_config.data_source_name, row)
                if doc is not None and len(doc) > 0:
                    self.original_docs[_id] = doc

                # if len(self.original_docs) % 100 == 0:
                print 'Original docs', len(self.original_docs)

        return True

    def get_updated_docs(self):
        load_config = self.get_load_config(updates_directory)
        ftp_manager = FTPManager(load_config)

        update_file_urls = ftp_manager.get_update_file_urls()
        update_file_urls = update_file_urls[:2]

        ftp_manager.download_missing_files(file_urls=update_file_urls, no_of_files=2)

        all_files = file_manager.get_all_files(load_config)
        files_to_process = all_files[:2]
        # files_to_process = file_manager.get_new_update_files(load_config, update_file_urls, 2)
        print files_to_process

        for update_file in files_to_process:
            file_name = os.path.basename(update_file)
            # self.current_update_file = file_name.split('.')[0]

            xml_data_source = XMLDataSource(update_file, 2)
            xml_data_source.process_rows(self.process_row)

        print 'Total updated ids:', len(self.updated_docs)

        file_utils.save_file(self.load_config.other_files_directory(), 'updated_docs.json', self.updated_docs)
        file_utils.save_file(self.load_config.other_files_directory(), 'inverted_index_for_updated_docs.json', self.inverted_index_for_updated_docs)


    def process_row(self, row, current_index):
        _id = self.extract_id(self.load_config.data_source_name, row, current_index)
        if _id is not None and _id not in self.updated_docs:
            doc = self.extract_data(_id, self.load_config.data_source_name, row)
            if doc is not None and len(doc) > 0:
                self.updated_docs[_id] = doc

            self.inverted_index_for_updated_docs[_id] = self.current_update_file

            if len(self.updated_docs) % 1000 == 0:
                print 'Updated docs', len(self.updated_docs)

        return True

    def get_load_config(self, root_directory):
        load_config = LoadConfig()
        load_config.root_directory = root_directory
        load_config.process_count = psutil.cpu_count()

        load_config.server = self.server
        load_config.server_username = self.server_username
        load_config.server_password = self.server_password
        load_config.index = self.index
        load_config.type = self.type

        load_config.data_mapper = self.get_data_mapper()
        load_config.data_extractor = self.get_data_extractor()
        load_config.max_memory_percent = self.get_max_memory_percent()

        return load_config


    def get_data_mapper(self):
        return PubmedDataMapper()

    def get_data_extractor(self):
        return PubmedDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def extract_id(self, name, row, current_index):
        if self.load_config.data_extractor is not None:
            if self.load_config.data_extractor.should_generate_id(name):
                return self.load_config.data_extractor.generate_id(current_index)
            else:
                return self.load_config.data_extractor.extract_id(name, row)

        self.load_config.log(LOG_LEVEL_WARNING, 'Error: no data extractor configured')
        return None

    def extract_data(self, _id, name, row):
        if self.load_config.data_extractor is not None:
            return self.load_config.data_extractor.extract_data(_id, name, row)

        return row

clean_citations = CleanCitations()
clean_citations.run()