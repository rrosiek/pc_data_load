from data_load.base.load_config import LoadConfig
from config import *
from data_load.base.constants import LOCAL_SERVER
from irdb_data_mapper import IRDBDataMapper
from irdb_data_extractor import IRDBDataExtractor
import psutil


reload_load_config = None

def get_load_config():
    global reload_load_config
    if reload_load_config is None:
        reload_load_config = create_load_config()
  
    return reload_load_config

def create_load_config():
    load_config = LoadConfig()
    load_config.root_directory = ROOT_DIRECTORY
    # load_config.data_source_name = 'extended_relations'
    load_config.process_count = psutil.cpu_count()

    load_config.server = LOCAL_SERVER
    load_config.index = INDEX
    load_config.type = TYPE

    load_config.data_mapper = IRDBDataMapper()
    load_config.data_extractor = IRDBDataExtractor()
    # load_config.data_source_name = file_name.split('.')[0]
    load_config.max_memory_percent = 75

    return load_config