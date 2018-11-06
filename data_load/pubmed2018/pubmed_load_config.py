from data_load.base.load_config import LoadConfig
from config import *
from data_load.pubmed2018.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2018.pubmed_data_mapper import PubmedDataMapper
from data_load.base.utils.logger import LOG_LEVEL_TRACE

def get_load_config():
    load_config = LoadConfig()
    load_config.root_directory = ROOT_DIRECTORY

    load_config.server = SERVER
    load_config.index = INDEX
    load_config.type = TYPE

    load_config.data_extractor = PubmedDataExtractor()
    load_config.data_mapper = PubmedDataMapper()
    # load_config.data_source_name = file_name.split('.')[0]

    # load_config.log_level = LOG_LEVEL_TRACE

    return load_config

