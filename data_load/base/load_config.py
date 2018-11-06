from constants import SOURCE_FILES_DIRECTORY
from constants import GENERATED_FILES_DIRECTORY
from constants import OTHER_FILES_DIRECTORY
from constants import LOG_FILES_DIRECTORY

from constants import FAILED_DOCS_DIRECTORY
from constants import LOADED_DOCS_DIRECTORY
from constants import BULK_UPDATE_RESPONSE_DIRECTORY

from constants import DATA_SOURCE_BATCH_SIZE
from constants import DATA_LOADER_BATCH_SIZE
from constants import DOC_FETCH_BATCH_SIZE

from constants import BULK_DATA_SIZE

from constants import INDEX_MAPPING
from constants import LOCAL_SERVER

from constants import PROCESS_SPAWN_DELAY
from constants import PROCESS_COUNT
from constants import MAX_RETRIES

from utils import file_utils
# from utils.logger import LOG_LEVEL_DEBUG, LOG_LEVEL_ERROR, LOG_LEVEL_FATAL, LOG_LEVEL_INFO, LOG_LEVEL_TRACE, LOG_LEVEL_WARNING
import utils.log_utils as log_utils
from utils.log_utils import *

import logging
import datetime
import time

current_milli_time = lambda: int(round(time.time() * 1000))



class LoadConfig(object):
    def __init__(self):
        # Configure
        self.root_directory = None

        self.data_mapper = None
        self.data_extractor = None
        self.data_source_name = None

        # ES Index & Type
        self.index = None
        self.type = None
        self.index_id = None

        # Relationships
        self.append_relations = True
        self.source = ''

        # Constants
        self.server = LOCAL_SERVER

        self.data_source_batch_size = DATA_SOURCE_BATCH_SIZE
        self.data_loader_batch_size = DATA_LOADER_BATCH_SIZE
        self.doc_fetch_batch_size = DOC_FETCH_BATCH_SIZE

        self.process_count = PROCESS_COUNT
        self.process_spawn_delay = PROCESS_SPAWN_DELAY

        self.bulk_data_size = BULK_DATA_SIZE

        self.log_level = LOG_LEVEL_DEBUG
        self.test_mode = False

        self.auto_retry_load = True
        self.max_retries = MAX_RETRIES

        self._logger = None
        self.last_time_stamp = 0
        self.log_delay = 1500  # milliseconds

        self.max_memory_percent = 85

    def save(self):
        file_utils.pickle_file(self.root_directory, 'load_config.pckl', self)

    def logger(self):
        if self._logger is None:
            self._logger = log_utils.create_logger(self.data_source_name, self.log_files_directory())

        return self._logger

    def log(self, log_level, *message):
        if log_level <= LOG_LEVEL_INFO or self.log_level >= LOG_LEVEL_TRACE or (current_milli_time() - self.last_time_stamp) >= self.log_delay:
            if self.log_level >= log_level:
                logger = self.logger()
                if log_level == LOG_LEVEL_TRACE:
                    logger.info('[' + self.data_source_name + ']' + ' ' + ' '.join((str(msg) for msg in message)))
                if log_level == LOG_LEVEL_INFO:
                    logger.info('[' + self.data_source_name + ']' + ' ' + ' '.join((str(msg) for msg in message)))
                if log_level == LOG_LEVEL_DEBUG:
                    logger.debug('[' + self.data_source_name + ']' + ' ' + ' '.join((str(msg) for msg in message)))
                if log_level == LOG_LEVEL_WARNING:
                    logger.warning('[' + self.data_source_name + ']' + ' ' + ' '.join((str(msg) for msg in message)))
                if log_level == LOG_LEVEL_ERROR:
                    logger.error('[' + self.data_source_name + ']' + ' ' + ' '.join((str(msg) for msg in message)))
                if log_level == LOG_LEVEL_FATAL:
                    logger.critical('[' + self.data_source_name + ']' + ' ' + ' '.join((str(msg) for msg in message)))

            self.last_time_stamp = current_milli_time()

    def set_logger(self, logger):
        self._logger = logger

    def generated_files_directory(self):
        generated_files_directory = GENERATED_FILES_DIRECTORY
        if self.root_directory is not None:
            generated_files_directory = self.root_directory + '/' + generated_files_directory

        file_utils.make_directory(generated_files_directory)
        return generated_files_directory

    def source_files_directory(self):
        source_files_directory = SOURCE_FILES_DIRECTORY
        if self.root_directory is not None:
            source_files_directory = self.root_directory + '/' + source_files_directory

        file_utils.make_directory(source_files_directory)
        return source_files_directory

    def other_files_directory(self):
        other_files_directory = OTHER_FILES_DIRECTORY
        if self.root_directory is not None:
            other_files_directory = self.root_directory + '/' + other_files_directory

        file_utils.make_directory(other_files_directory)
        return other_files_directory

    def log_files_directory(self):
        log_files_directory = LOG_FILES_DIRECTORY
        if self.root_directory is not None:
            log_files_directory = self.root_directory + '/' + log_files_directory

        file_utils.make_directory(log_files_directory)
        return log_files_directory

    def data_source_directory(self, data_source_name=None):
        data_source_directory = self.generated_files_directory()
        if data_source_name is not None:
            data_source_directory = data_source_directory + '/' + data_source_name
        elif self.data_source_name is not None:
            data_source_directory = data_source_directory + '/' + self.data_source_name

        file_utils.make_directory(data_source_directory)
        return data_source_directory

    def data_source_batch_directory(self, data_source_batch_name):
        data_source_batch_directory = self.data_source_directory()
        if data_source_batch_name is not None:
            data_source_batch_directory = data_source_batch_directory + '/' + data_source_batch_name

        file_utils.make_directory(data_source_batch_directory)
        return data_source_batch_directory

    def failed_docs_directory(self, data_source_batch_name):
        data_source_batch_directory = self.data_source_batch_directory(data_source_batch_name)
        failed_docs_directory = data_source_batch_directory + '/' + FAILED_DOCS_DIRECTORY
    
        file_utils.make_directory(failed_docs_directory)
        return failed_docs_directory

    def loaded_docs_directory(self, data_source_batch_name):
        data_source_batch_directory = self.data_source_batch_directory(data_source_batch_name)
        loaded_docs_directory = data_source_batch_directory + '/' + LOADED_DOCS_DIRECTORY

        file_utils.make_directory(loaded_docs_directory)
        return loaded_docs_directory

    def bulk_update_response_directory(self, data_source_batch_name):
        data_source_batch_directory = self.data_source_batch_directory(data_source_batch_name)
        bulk_update_response_directory = data_source_batch_directory + '/' + BULK_UPDATE_RESPONSE_DIRECTORY

        file_utils.make_directory(bulk_update_response_directory)
        return bulk_update_response_directory

    @staticmethod
    def index_for_index_id(index_id):
        return INDEX_MAPPING[index_id]['index']

    @staticmethod
    def type_for_index_id(index_id):
        return INDEX_MAPPING[index_id]['type']
