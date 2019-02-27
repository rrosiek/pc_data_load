from data_load.base.load_config import LoadConfig
import data_load.irdb.irdb_load_config as irdb_load_config_getter

from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.base.utils.logger import LOG_LEVEL_TRACE
from data_load.base.constants import ID_PUBMED, INDEX_MAPPING

def get_load_config():
    irdb_load_config = irdb_load_config_getter.get_load_config()
    load_config = LoadConfig()
    load_config.root_directory = irdb_load_config.root_directory

    load_config.server = irdb_load_config.server
    load_config.index = INDEX_MAPPING[ID_PUBMED]['index']
    load_config.type = INDEX_MAPPING[ID_PUBMED]['type']

    load_config.data_extractor = PubmedDataExtractor()
    load_config.data_mapper = PubmedDataMapper()
    # load_config.data_source_name = file_name.split('.')[0]
    load_config.process_count = irdb_load_config.process_count

    # load_config.log_level = LOG_LEVEL_TRACE

    return load_config

