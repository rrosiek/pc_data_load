import os
import psutil

from utils import file_utils
from constants import DATA_LOADER_BATCH_PREFIX
from constants import DATA_SOURCE_BATCH_PREFIX
from constants import DATA_SOURCE_BATCHES_FILE

from data_load_batcher import DataLoadBatcher

from load_config import *

import gc


class DataSourceProcessor(object):
    MODE_NORMAL_LOAD = 'MODE_NORMAL_LOAD'
    MODE_RETRY_DATA_SOURCE = 'MODE_RETRY_DATA_SOURCE'
    MODE_RETRY_FAILED_DOCS = 'MODE_RETRY_FAILED_DOCS'

    def __init__(self, load_config, data_source):
        self.load_config = load_config
        self.data_source = data_source
        self.data_source_batch = {}

        self.start_index = 0
        self.total_rows = 0
        self.previous_id = None

        self.processed_indices = {}
        self.failed_ids = {}

        self.mode = DataSourceProcessor.MODE_NORMAL_LOAD
        self.load_relationships = False

        self.process_ids_method = self.default_process_ids_method

    def load_data_source_batches(self):
        data_source_directory = self.load_config.data_source_directory()
        return file_utils.load_file(data_source_directory, DATA_SOURCE_BATCHES_FILE)

    def save_data_source_batches(self, data_source_batches):
        data_source_directory = self.load_config.data_source_directory()
        file_utils.save_file(data_source_directory,
                             DATA_SOURCE_BATCHES_FILE, data_source_batches)

    def extract_data(self, _id, name, row):
        if self.load_config.data_extractor is not None:
            return self.load_config.data_extractor.extract_data(_id, name, row)

        return row

    def extract_id(self, name, row, current_index):
        if self.load_config.data_extractor is not None:
            if self.load_config.data_extractor.should_generate_id(name):
                return self.load_config.data_extractor.generate_id(current_index)
            else:
                return self.load_config.data_extractor.extract_id(name, row)

        self.load_config.log(
            LOG_LEVEL_WARNING, 'Error: no data extractor configured')
        return None

    def memory_usage_psutil(self):
        # return the memory usage in percentage like top
        process = psutil.Process(os.getpid())
        mem = process.memory_percent()
        return mem

    def should_split_batch(self, current_index, _id, previous_id=None):
        if previous_id is not None:
            memory_details = psutil.virtual_memory()
            if memory_details.percent >= self.load_config.max_memory_percent or current_index % self.load_config.data_source_batch_size == 0:
                # or (len(self.data_source_batch) >=
                # self.load_config.data_source_batch_size > 0):
                if _id != previous_id:
                    self.load_config.log(LOG_LEVEL_INFO, 'memory_details.percent', memory_details.percent,
                                         'data_source_batch',
                                         len(self.data_source_batch),
                                         'load_config.data_source_batch_size',
                                         self.load_config.data_source_batch_size)
                    return True
                else:
                    return False
        return False

    def clear_data_source_batch(self):
        self.data_source_batch = {}

    def start_processing(self):
        self.load_config.log(LOG_LEVEL_INFO, 'Start processing...')

        self.start_index = 0
        self.previous_id = None
        self.clear_data_source_batch()

        if self.mode == self.MODE_NORMAL_LOAD:
            self.processed_indices = self.get_processed_indices()
        elif self.mode == self.MODE_RETRY_FAILED_DOCS:
            self.failed_ids = self.get_failed_ids()
        else:
            self.processed_indices = {}
            self.failed_ids = {}

    def end_processing(self):
        self.load_config.log(LOG_LEVEL_INFO, 'End processing...')

        if len(self.data_source_batch) > 0:
            self.process_batch()

    def load_stats(self):
        self.load_config.log(LOG_LEVEL_INFO, 'Loading stats...')
        data_source_directory = self.load_config.data_source_directory()
        stats = file_utils.load_file(data_source_directory, 'stats.json')

        if 'total_rows' in stats:
            self.total_rows = stats['total_rows']
  

    def save_stats(self):
        self.load_config.log(LOG_LEVEL_INFO, 'Saving stats...')

        stats = {
            'total_rows': self.total_rows,
            'total_ids': len(self.data_source_batch)
        }

        self.load_config.log(LOG_LEVEL_INFO, stats)

        data_source_directory = self.load_config.data_source_directory()
        file_utils.save_file(data_source_directory,
                             'unique_ids.json', self.data_source_batch)
        file_utils.save_file(data_source_directory, 'stats.json', stats)

    def run(self, process_ids_method=None):
        if process_ids_method is not None:
            self.process_ids_method = process_ids_method

        # Delete the
        if self.mode == self.MODE_RETRY_DATA_SOURCE:
            self.delete_history()

        self.data_source.initialize()
        self.load_stats()


        if self.total_rows == 0:
            self.load_config.log(
                LOG_LEVEL_INFO, 'data source batch size is zero, counting rows...')
            # Count rows
            self.data_source.process_rows(self.count_row)
            # Save stats
            self.save_stats()

        # raw_input('Continue?')

        # Start processing
        self.start_processing()
        self.data_source.process_rows(self.process_row)
        self.end_processing()

    def delete_history(self):
        data_source_directory = self.load_config.data_source_directory()
        for name in os.listdir(data_source_directory):
            file_path = os.path.join(data_source_directory, name)
            if DATA_SOURCE_BATCH_PREFIX in name:
                os.remove(file_path)

    def get_processed_indices(self):
        self.load_config.log(LOG_LEVEL_INFO, 'Fetching processed ids...')
        processed_indices = {}
        data_source_directory = self.load_config.data_source_directory()
        for name in os.listdir(data_source_directory):
            file_path = os.path.join(data_source_directory, name)
            if os.path.isfile(file_path) and name.startswith(DATA_SOURCE_BATCH_PREFIX):
                batch_info = file_utils.load_file(data_source_directory, name)
                if 'processed_indices' in batch_info:
                    batch_processed_indices = batch_info['processed_indices']
                    batch_processed_indices = dict.fromkeys(batch_processed_indices, None)
                    processed_indices.update(batch_processed_indices)

        self.load_config.log(
            LOG_LEVEL_INFO, 'Processed indices', len(processed_indices))

        return processed_indices

    def get_failed_ids(self):
        self.load_config.log(LOG_LEVEL_INFO, 'Fetching failed ids...')

        failed_ids = {}
        data_source_directory = self.load_config.data_source_directory()
        for name in os.listdir(data_source_directory):
            file_path = os.path.join(data_source_directory, name)
            if os.path.isfile(file_path) and name.startswith(DATA_SOURCE_BATCH_PREFIX):
                batch_info = file_utils.load_file(data_source_directory, name)
                batch_failed_ids = batch_info['failed_ids']
                batch_failed_ids = dict.fromkeys(batch_failed_ids, None)

                failed_ids.update(batch_failed_ids)

        self.load_config.log(LOG_LEVEL_INFO, 'Failed ids', len(failed_ids))

        return failed_ids

    def count_row(self, row, current_index):
        _id = self.extract_id(
            self.load_config.data_source_name, row, current_index)
        if _id is not None:
            self.load_config.log(
                LOG_LEVEL_DEBUG, 'Counting rows', current_index + 1, len(self.data_source_batch))

            if self.mode == self.MODE_NORMAL_LOAD and _id in self.processed_indices:
                return True

            if self.mode == self.MODE_RETRY_FAILED_DOCS and _id not in self.failed_ids:
                return True

            if _id not in self.data_source_batch:
                self.data_source_batch[_id] = []

        self.total_rows = current_index
        return True

    def process_row(self, row, current_index):
        if current_index >= self.start_index:
            _id = self.extract_id(
                self.load_config.data_source_name, row, current_index)
            if _id is not None:
                # Check and split batch
                if self.should_split_batch(current_index, _id, self.previous_id):
                    self.process_batch()
                    time.sleep(5)
                    collected = gc.collect()
                    self.load_config.log(LOG_LEVEL_DEBUG, 'Waiting...')

                    self.start_index = current_index
                    self.load_config.log(
                        LOG_LEVEL_INFO, 'Processing from', current_index)

                self.load_config.log(
                    LOG_LEVEL_DEBUG, 'Processed rows', current_index + 1, self.total_rows)

                if self.mode == self.MODE_NORMAL_LOAD and current_index in self.processed_indices:
                    return True

                if self.mode == self.MODE_RETRY_FAILED_DOCS and _id not in self.failed_ids:
                    return True

                # Process doc
                doc = self.extract_data(
                    _id, self.load_config.data_source_name, row)
                # self.load_config.log(LOG_LEVEL_ERROR, 'Extracted doc', doc)
                if doc is not None and len(doc) > 0:
                    if _id not in self.data_source_batch:
                        self.data_source_batch[_id] = []

                    self.data_source_batch[_id].append(doc)

                    if self.load_config.test_mode and current_index % 10000 == 0:
                        self.load_config.log(LOG_LEVEL_INFO, _id, doc)
                else:
                    self.load_config.log(
                        LOG_LEVEL_DEBUG, 'Null doc', current_index, _id)

                self.previous_id = _id

        return True

    def process_relationships(self, extracted_ids):
        relationships = self.load_config.data_mapper.process_relationships(
            self.load_config, extracted_ids)
        return relationships

    def process_and_load_relationships(self, extracted_ids):
        relationships = self.process_relationships(extracted_ids)
        data_source_batch_name = self.data_source_batch_name()
        self.start_load_relationships(data_source_batch_name, relationships)

    def process_batch(self):
        # Extract ids for batch
        extracted_ids = self.extract_ids_from_batch()

        if self.load_relationships:
            self.process_and_load_relationships(extracted_ids)
        else:
            data_source_batch_name = self.data_source_batch_name()
            self.process_ids(extracted_ids, self.load_config.index, self.load_config.type, data_source_batch_name)

    def start_load_relationships(self, data_source_batch_name, relationships):
        for source_index_id in relationships:
            _index = self.load_config.index_for_index_id(source_index_id)
            _type = self.load_config.type_for_index_id(source_index_id)
            batch_name = data_source_batch_name + '_' + source_index_id
            self.process_ids(
                relationships[source_index_id], _index, _type, batch_name)

    def process_ids(self, ids, _index, _type, data_source_batch_name):
        # Process batch ids
        batch_ids = ids.keys()
        self.load_config.log(LOG_LEVEL_INFO, 'Processing batch: ',
                             data_source_batch_name, ',', len(batch_ids), 'docs')

        data_source_batches = self.load_data_source_batches()
        data_source_batches[data_source_batch_name] = 0
        self.save_data_source_batches(data_source_batches)

        failed_ids = self.process_ids_method(
            ids, _index, _type, data_source_batch_name)

        processed_indices = []
        for _id in batch_ids:
            processed_indices.append(_id)

        # Save batch info
        batch_info = {
            'batch_ids': batch_ids,
            'processed_indices':processed_indices,
            'failed_ids': failed_ids
        }

        # print 'Batch ids', len(ids)
        # print 'Saving batch info', batch_info

        data_source_directory = self.load_config.data_source_directory()
        file_utils.save_file(data_source_directory,
                             data_source_batch_name + '.json', batch_info)

    def default_process_ids_method(self, ids, _index, _type, data_source_batch_name):
        data_load_batcher = DataLoadBatcher(self.load_config, _index, _type)
        data_load_batcher.load_relationships = self.load_relationships
        failed_ids = data_load_batcher.process_data_rows(
            data_source_batch_name, ids)
        return failed_ids

    def extract_ids_from_batch(self):
        self.load_config.log(LOG_LEVEL_INFO, 'Extracting ids...')
        extracted_ids = {}
        data_source_batch_keys = self.data_source_batch.keys()
        for _id in data_source_batch_keys:
            data_for_id = self.data_source_batch.pop(_id)
            extracted_ids[_id] = data_for_id

        self.load_config.log(
            LOG_LEVEL_INFO, 'Extracted ids', len(extracted_ids))

        return extracted_ids

    def data_source_batch_name(self):
        data_source_batch_name = file_utils.batch_file_name_with_prefix(
            DATA_SOURCE_BATCH_PREFIX)
        return data_source_batch_name

    def get_data_source_batch_summary(self, data_source_batch_name):
        failed_docs_directory = self.load_config.failed_docs_directory(
            data_source_batch_name)
        loaded_docs_directory = self.load_config.loaded_docs_directory(
            data_source_batch_name)

        updated_ids = []
        indexed_ids = []
        failed_ids = []

        for data_loader_batch_name in os.listdir(failed_docs_directory):
            data_loader_batch_path = os.path.join(
                failed_docs_directory, data_loader_batch_name)
            if os.path.isfile(data_loader_batch_path) and data_loader_batch_name.startswith(DATA_LOADER_BATCH_PREFIX):
                batch_failed_docs = file_utils.load_file(
                    failed_docs_directory, data_loader_batch_name)
                failed_ids.extend(batch_failed_docs.keys())

        for data_loader_batch_name in os.listdir(loaded_docs_directory):
            data_loader_batch_path = os.path.join(
                loaded_docs_directory, data_loader_batch_name)
            if os.path.isfile(data_loader_batch_path) and data_loader_batch_name.startswith(DATA_LOADER_BATCH_PREFIX):
                data_loader_batch = file_utils.load_file(
                    loaded_docs_directory, data_loader_batch_name)

                updated_ids.extend(data_loader_batch['updated_ids'])
                indexed_ids.extend(data_loader_batch['indexed_ids'])

        summary = dict()
        summary['updated_ids'] = updated_ids
        summary['indexed_ids'] = indexed_ids
        summary['failed_ids'] = failed_ids

        return summary

    def get_combined_data_source_summary(self):
        updated_ids = {}
        indexed_ids = {}
        failed_ids = {}

        data_source_directory = self.load_config.data_source_directory()
        data_source_batches = self.load_data_source_batches()

        for data_source_batch_name in data_source_batches:
            data_source_batch_summary = self.get_data_source_batch_summary(
                data_source_batch_name)

            # Remove duplicates across batches
            for _id in data_source_batch_summary['updated_ids']:
                updated_ids[_id] = 0
            for _id in data_source_batch_summary['indexed_ids']:
                indexed_ids[_id] = 0
            for _id in data_source_batch_summary['failed_ids']:
                failed_ids[_id] = 0

        # failed_ids can be present in updated_ids or indexed_ids arrays.
        # filter those out
        filtered_failed_ids = {}
        for _id in failed_ids:
            if _id not in updated_ids and _id not in indexed_ids:
                filtered_failed_ids[_id] = 0

        # Load the data source stats
        data_source_summary = file_utils.load_file(
            data_source_directory, 'stats.json')

        summary = dict()
        summary['total_rows'] = data_source_summary['total_rows']
        summary['total_ids'] = data_source_summary['total_ids']

        summary['updated_ids'] = updated_ids
        summary['indexed_ids'] = indexed_ids
        summary['failed_ids'] = filtered_failed_ids

        return summary
