from data_load.base.load_config import LoadConfig
from config import *
from data_load.base.utils.logger import LOG_LEVEL_TRACE
from data_load.clinical_trials.ct_data_extractor import CTDataExtractor
from data_load.clinical_trials.ct_data_mapper import CTDataMapper


def get_load_config():
    load_config = LoadConfig()
    load_config.root_directory = ROOT_DIRECTORY

    load_config.server = SERVER
    load_config.index = INDEX
    load_config.type = TYPE

    load_config.process_count = PROCESS_COUNT
    load_config.bulk_data_size = BULK_DATA_SIZE
    load_config.data_loader_batch_size = DATA_LOADER_BATCH_SIZE
    load_config.data_source_batch_size = DATA_SOURCE_BATCH_SIZE
    load_config.doc_fetch_batch_size = DOC_FETCH_BATCH_SIZE

    # load_config.log_level = LOG_LEVEL_TRACE

    load_config.data_extractor = CTDataExtractor()
    load_config.data_mapper = CTDataMapper()
    # load_config.data_source_name = file_name.split('.')[0]

    load_config.max_memory_percent = 80

    return load_config
