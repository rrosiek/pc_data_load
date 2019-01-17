
from data_load.base.constants import DATA_LOADING_DIRECTORY, ID_PUBMED
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig
from data_load.base.data_source_xml import XMLDataSource
from data_load.base.utils.data_loader_utils import DataLoaderUtils

from data_load.pubmed.ftp_manager import FTPManager
import data_load.pubmed.file_manager as file_manager

import psutil
import threading
import sys

ID_PUBMED_2019 = 'PUBMED_2019'

baseline_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'pubmed2019'
updates_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'pubmed2019_updates'
clean_citations_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'clean_citations'

SERVER = 'http://localhost:9200'
INDEX = 'pubmed2019'
TYPE = 'article'

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

    def run(self):
        # self.get_updated_docs()
        self.get_original_docs()

        print 'Updated docs:', len(self.updated_docs)
        print 'Original docs:', len(self.original_docs)
        input = raw_input('Continue?')
        if input.lower() in ['n', 'no', '0']:
            sys.exit(1)
        self.update_docs()

        print 'Docs with updates', len(self.docs_with_updates)
        print self.docs_with_updates

    def update_docs(self):
        for _id in self.updated_docs:
            if _id in self.original_docs:
                original_doc = self.original_docs[_id]
                updated_doc = self.update_doc[_id]
                original_citations = self.load_config.data_mapper.get_citations(original_doc)
                updated_citations = self.load_config.data_mapper.get_citations(updated_doc)

                print _id, 'original', len(original_citations), 'updated', len(updated_citations)
                if not self.compare_citations():
                    self.docs_with_updates[_id] = 0
                # self.update_doc(_id, original_citations)

    def compare_citations(self, original_citations, updated_citations):
        for _id in original_citations:
            if _id not in updated_citations:
                return False

        for _id in updated_citations:
            if _id not in original_citations:
                return False

        return True

    def update_doc(self, _id, original_citations):
        print 'Updating doc', _id, len(original_citations), 'citations'
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

        threads = []
        for baseline_file in baseline_files:
            t = threading.Thread(target=self.process_baseline_file, args=(baseline_file,)) 
            t.start()
            threads.append(t)

            if len(threads) >= 48:
                t1 = threads.pop(0)
                t1.join()

        while len(threads) > 0:
            t1 = threads.pop(0)
            t1.join()
                
    def process_baseline_file(self, baseline_file):
        print "Processing file:", baseline_file
        xml_data_source = XMLDataSource(baseline_file, 2)
        xml_data_source.process_rows(self.process_baseline_row)

    def process_baseline_row(self, row, current_index):
        _id = self.extract_id(self.load_config.data_source_name, row, current_index)
        if _id is not None and _id in self.updated_docs:
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
            xml_data_source = XMLDataSource(update_file, 2)
            xml_data_source.process_rows(self.process_row)

        print 'Total updated ids:', len(self.updated_docs)

    def process_row(self, row, current_index):
        _id = self.extract_id(self.load_config.data_source_name, row, current_index)
        if _id is not None and _id not in self.updated_docs:
            doc = self.extract_data(_id, self.load_config.data_source_name, row)
            if doc is not None and len(doc) > 0:
                self.updated_docs[_id] = doc

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