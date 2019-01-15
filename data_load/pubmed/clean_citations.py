
from data_load.base.constants import DATA_LOADING_DIRECTORY, ID_PUBMED
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig
from data_load.base.data_source_xml import XMLDataSource

from data_load.pubmed.ftp_manager import FTPManager
import data_load.pubmed.file_manager as file_manager

import psutil

ID_PUBMED_2019 = 'PUBMED_2019'

baseline_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'pubmed2019'
updates_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'pubmed2019_updates'
clean_citations_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'clean_citations'

SERVER = 'http://localhost:9200'
INDEX = 'pubmed2019'
TYPE = 'article'

class CleanCitations(object):


    def __init__(self):
        self.ids = {}

        self.server = SERVER
        self.index = INDEX
        self.type = TYPE

        self.server_username = ''
        self.server_password = ''

        self.load_config = self.get_load_config(clean_citations_directory)

    def run(self):
        load_config = self.get_load_config(updates_directory)
        ftp_manager = FTPManager(load_config)

        update_file_urls = ftp_manager.get_update_file_urls()
        update_file_urls = update_file_urls[:2]

        ftp_manager.download_missing_files(file_urls=update_file_urls, no_of_files=2)

        all_files = file_manager.get_all_files(load_config)
        files_to_process = all_files[:2]
        # files_to_process = file_manager.get_new_update_files(load_config, update_file_urls, 2)

        for update_file in files_to_process:
            xml_data_source = XMLDataSource(update_file, 2)
            xml_data_source.process_rows(self.process_row)

        print files_to_process

    def process_row(self, row, current_index):
        _id = self.extract_id(self.load_config.data_source_name, row, current_index)
        if _id is not None and _id not in self.ids:
            self.ids[_id] = 0

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


clean_citations = CleanCitations()
clean_citations.run()