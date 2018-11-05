from data_load.base.data_processor import DataProcessor
from data_load.base.data_source_xml import XMLDataSource
from data_load.base.load_manager import LoadManager

from config import *
from pubmed_data_extractor import PubmedDataExtractor
from pubmed_data_mapper import PubmedDataMapper

import os


class PubmedLoadManager(LoadManager):

    def process_file(self, file_path):
        file_name = os.path.basename(file_path)

        data_processor = DataProcessor(XMLDataSource(file_path, 2))
        data_processor.load_config.root_directory = ROOT_DIRECTORY

        data_processor.load_config.server = 'http://localhost:9200'
        data_processor.load_config.index = INDEX
        data_processor.load_config.type = TYPE

        data_processor.load_config.data_extractor = PubmedDataExtractor()
        data_processor.load_config.data_mapper = PubmedDataMapper()

        data_processor.load_config.data_source_name = file_name.split('.')[0]

        data_processor.process_rows()

    def run(self):
        pass

