import os
import time
import psutil
from multiprocessing import Process

from data_loader import DataLoader
from utils import file_utils

from load_config import LoadConfig
from constants import DATA_LOADER_BATCH_PREFIX
from constants import DATA_SOURCE_BATCH_PREFIX

from load_config import *

class DataProcessor(object):

    MODE_NORMAL_LOAD = 'MODE_NORMAL_LOAD'
    MODE_RETRY_DATA_SOURCE = 'MODE_RETRY_DATA_SOURCE'
    MODE_RETRY_FAILED_DOCS = 'MODE_RETRY_FAILED_DOCS'

    def __init__(self, load_config, data_source):
        self.load_config = load_config
        self.data_source = data_source
        self.data_source_batch = {}

        self.start_index = 0
        self.count = 0
        self.previous_id = None

        self.processes = []

        self.mode = DataProcessor.MODE_NORMAL_LOAD

        self.data_source_stats = None

    def extract_data(self,  _id, name, row):
        if self.load_config.data_extractor is not None:
            return self.load_config.data_extractor.extract_data(_id, name, row)

        return row

    def extract_id(self, name, row):
        if self.load_config.data_extractor is not None:
            return self.load_config.data_extractor.extract_id(name, row)

        self.load_config.log(LOG_LEVEL_WARNING, 'Error: no data extractor configured')
        return None

    def start_processing(self):
        if self.mode == DataProcessor.MODE_NORMAL_LOAD:
            self.start_index = self.get_processed_rows()
        elif self.mode == DataProcessor.MODE_RETRY_DATA_SOURCE or self.mode == DataProcessor.MODE_RETRY_FAILED_DOCS:
            self.start_index = 0
        self.previous_id = None
        self.count = 0
        self.clear_data_source_batch()

    def end_processing(self):
        if len(self.data_source_batch) > 0:
            self.process_batch(self.start_index, self.count)

        self.join_processes()

    def should_split_batch(self, start_index, count, _id, previous_id=None):
        if previous_id is not None:
            memory_details = psutil.virtual_memory()
            if memory_details.percent >= 95 or (
                    len(self.data_source_batch) >= self.load_config.data_source_batch_size > 0):
                if _id != previous_id:
                    return True
                else:
                    return False
        return False

    def data_source_stats_file_name(self):
        return self.load_config.data_source_name + '_stats.json'

    def count_rows(self):
        data_source_directory = self.load_config.data_source_directory()
        stats_file_name = self.data_source_stats_file_name()
        self.data_source_stats = file_utils.load_file(data_source_directory, stats_file_name)
        if self.data_source_stats is None or len(self.data_source_stats) == 0:
            self.count = 0
            self.data_source_batch = {}
            self.data_source.process_rows(0, self.count_row)
            self.load_config.log(LOG_LEVEL_INFO, 'Total rows:', self.count)
            self.load_config.log(LOG_LEVEL_INFO, 'Total ids:', len(self.data_source_batch))

            self.data_source_stats = {
                'row_count': self.count,
                'unique_ids': len(self.data_source_batch)
            }
            file_utils.save_file(data_source_directory, stats_file_name, self.data_source_stats)

    def get_progress(self):
        data_source_directory = self.load_config.data_source_directory()
        stats_file_name = self.data_source_stats_file_name()

        if self.data_source_stats is None:
            self.data_source_stats = file_utils.load_file(data_source_directory, stats_file_name)

        row_count = 0
        unique_ids = 0
        # if 'row_count' in self.data_source_stats:
        #     row_count = self.data_source_stats['row_count']
        if 'unique_ids' in self.data_source_stats:
            unique_ids = self.data_source_stats['unique_ids']

        docs_loaded = self.get_loaded_doc_count()

        if unique_ids > 0:
            self.load_config.log(LOG_LEVEL_INFO, 'docs loaded', docs_loaded, 'unique_ids', unique_ids)
            progress = (docs_loaded / float(unique_ids)) * 100
            self.data_source_stats['progress'] = progress
            file_utils.save_file(data_source_directory, stats_file_name, self.data_source_stats)
            return progress

        return -1

    def count_row(self, row, count):
        _id = self.extract_id(self.load_config.data_source_name, row)
        if _id is not None:
            self.data_source_batch[_id] = 0
            self.count = count
            self.load_config.log(LOG_LEVEL_DEBUG, 'Counting rows', count, len(self.data_source_batch))

        return True

    def process_rows(self):
        self.count_rows()
        self.start_processing()
        self.data_source.process_rows(self.start_index, self.process_row)
        self.end_processing()
        self.save_summary()

    def process_row(self, row, count):
        if count >= self.start_index:
            _id = self.extract_id(self.load_config.data_source_name, row)
            if _id is not None:
                if self.should_split_batch(self.start_index, count, _id, self.previous_id):
                    self.process_batch(self.start_index, count)
                    self.start_index = count

                doc = self.extract_data(_id, self.load_config.data_source_name, row)
                if doc is not None and len(doc) > 0:
                    if _id not in self.data_source_batch:
                        self.data_source_batch[_id] = []
                    self.data_source_batch[_id].append(doc)

                    if self.load_config.test_mode and count % 10000 == 0:
                        self.load_config.log(LOG_LEVEL_INFO, _id, doc)
                else:
                    self.load_config.log(LOG_LEVEL_ERROR, 'Null doc', self.count, _id)

                self.previous_id = _id
                self.count = count

                self.load_config.log(LOG_LEVEL_DEBUG, 'Processed rows', count, len(self.data_source_batch))

        return True

    def get_data_source_batch_name(self):
        data_source_directory = self.load_config.data_source_directory()
        data_source_batch_names = []
        for name in os.listdir(data_source_directory):
            file_path = os.path.join(data_source_directory, name)
            if not os.path.isfile(file_path) and name.startswith(DATA_SOURCE_BATCH_PREFIX) and not name.endswith('_retry'):
                data_source_batch_names.append(name)

        if len(data_source_batch_names) > 0:
            return data_source_batch_names[0]

        return None

    def data_source_batch_name(self, start_index, row_count):
        data_source_batch_name = DATA_SOURCE_BATCH_PREFIX + '_' + str(start_index) + '_' + str(row_count)
        return data_source_batch_name

    def process_batch(self, start_index, row_count):
        self.load_config.log(LOG_LEVEL_INFO, 'Split batch at', start_index, 'to', row_count, ', Doc count:', len(self.data_source_batch))

        data_source_batch_name = self.data_source_batch_name(start_index, row_count)
        unique_ids = self.data_source_batch.keys()
        self.process_data_rows(data_source_batch_name)
        self.join_processes()
        self.save_batch_info(start_index, row_count, unique_ids, data_source_batch_name)

        self.load_config.log(LOG_LEVEL_DEBUG, 'Clearing data source batch')
        self.clear_data_source_batch()

    def clear_data_source_batch(self):
        self.data_source_batch = {}

    def save_batch_info(self, start_index, row_count, unique_ids, data_source_batch_name):
        data_source_directory = self.load_config.data_source_directory()

        self.load_config.log(LOG_LEVEL_DEBUG,  'Finished processing batches, saving batch data...')
        row_count = row_count - start_index
        batch_data = {} #self.get_data_source_batch_summary(data_source_batch_name)
        batch_data['start_index'] = start_index
        batch_data['row_count'] = row_count
        batch_data['unique_ids'] = unique_ids

        if not self.load_config.test_mode:
            file_utils.save_file(data_source_directory, data_source_batch_name + '.json', batch_data)
            self.load_config.log(LOG_LEVEL_INFO, 'Saved batch data', data_source_batch_name)

    def get_processed_rows(self):
        data_source_directory = self.load_config.data_source_directory()

        processed_rows = 0
        for name in os.listdir(data_source_directory):
            file_path = os.path.join(data_source_directory, name)
            if os.path.isfile(file_path) and name.startswith(DATA_SOURCE_BATCH_PREFIX):
                self.load_config.log(LOG_LEVEL_DEBUG, 'processing file:', file_path)
                batch_data = file_utils.load_file(data_source_directory, name)
                start_index = batch_data['start_index']
                row_count = batch_data['row_count']

                end_index = start_index + row_count
                if end_index > processed_rows:
                    processed_rows = end_index

        return processed_rows

    def get_loaded_ids(self, data_loader_batch_directory):
        loaded_ids = {}
        for name in os.listdir(data_loader_batch_directory):
            file_path = os.path.join(data_loader_batch_directory, name)
            if os.path.isfile(file_path) and name.startswith(DATA_LOADER_BATCH_PREFIX):
                self.load_config.log(LOG_LEVEL_TRACE, 'processing file:', file_path)
                batch_data = file_utils.load_file(data_loader_batch_directory, name)
                updated_ids = batch_data['updated_ids']
                indexed_ids = batch_data['indexed_ids']

                for _id in updated_ids:
                    loaded_ids[_id] = 0

                for _id in indexed_ids:
                    loaded_ids[_id] = 0

        self.load_config.log(LOG_LEVEL_DEBUG, 'Loaded ids', len(loaded_ids))

        return loaded_ids

    def get_failed_ids(self, data_loader_batch_directory):
        failed_ids = {}

        for name in os.listdir(data_loader_batch_directory):
            file_path = os.path.join(data_loader_batch_directory, name)
            if os.path.isfile(file_path) and name.startswith("failed_docs_"):
                self.load_config.log(LOG_LEVEL_TRACE, 'processing file:', file_path)

                failed_docs = file_utils.load_file(data_loader_batch_directory, name)
                for _id in failed_docs:
                    failed_ids[_id] = failed_docs[_id]

        self.load_config.log(LOG_LEVEL_DEBUG, 'Failed ids', len(failed_ids))

        return failed_ids

    # def process_failed_docs(self):
    #     data_source_directory = self.load_config.data_source_directory()
    #
    #     self.load_config.log(LOG_LEVEL_INFO, 'Finding failed docs')
    #
    #     data_source_batch_names = []
    #     for name in os.listdir(data_source_directory):
    #         file_path = os.path.join(data_source_directory, name)
    #         if not os.path.isfile(file_path) and name.startswith(DATA_SOURCE_BATCH_PREFIX) and 'old' not in name:
    #             data_source_batch_names.append(name)
    #
    #     data_source_batch_names.sort()
    #
    #     for data_source_batch_name in data_source_batch_names:
    #         data_loader_batch_directory = data_source_directory + '/' + data_source_batch_name
    #         file_utils.make_directory(data_loader_batch_directory)
    #
    #         batch = {}
    #         for name in os.listdir(data_loader_batch_directory):
    #             file_path = os.path.join(data_loader_batch_directory, name)
    #             if os.path.isfile(file_path) and name.startswith("failed_docs_"):
    #                 self.load_config.log(LOG_LEVEL_DEBUG, 'Processing file:', file_path)
    #                 failed_docs = file_utils.load_file(data_loader_batch_directory, name)
    #
    #                 for _id in failed_docs:
    #                     doc = failed_docs[_id]['doc']
    #                     batch[_id] = doc
    #
    #         if len(batch) > 0:
    #             if self.mode is not DataProcessor.MODE_NORMAL_LOAD:
    #                 batch_id = str(int(round(time.time() * 1000)))
    #                 old_data_loader_batch_directory = data_source_directory + '/' + 'old_' + data_source_batch_name + '_' + batch_id
    #                 os.rename(data_loader_batch_directory, old_data_loader_batch_directory)
    #
    #             self.load_config.log(LOG_LEVEL_INFO, 'failed ids', len(batch))
    #             self.start_load_process(batch, data_source_batch_name)

    def process_data_rows(self, data_source_batch_name):
        data_source_directory = self.load_config.data_source_directory()
        data_source_batch_directory = self.load_config.data_source_batch_directory(data_source_batch_name)

        filtered_ids = []
        if self.mode == DataProcessor.MODE_RETRY_FAILED_DOCS or self.mode == DataProcessor.MODE_NORMAL_LOAD:
            loaded_ids = self.get_loaded_ids(data_source_batch_directory)
        else:
            loaded_ids = {}

        # filter ids
        for _id in self.data_source_batch:
            if _id not in loaded_ids:
                filtered_ids.append(_id)

        if self.mode == DataProcessor.MODE_RETRY_DATA_SOURCE:
            batch_id = str(int(round(time.time() * 1000)))
            old_data_source_batch_directory = data_source_directory + '/' + 'old_' + data_source_batch_name + '_' + batch_id
            os.rename(data_source_batch_directory, old_data_source_batch_directory)

        batch = {}
        count = 0
        for _id in filtered_ids:
            data = self.data_source_batch.pop(_id, None)
            batch[_id] = data
            count += 1

            if count % self.load_config.data_loader_batch_size == 0:
                self.start_load_process(batch, data_source_batch_name)
                batch = {}

        if len(batch) > 0:
            self.start_load_process(batch, data_source_batch_name)

    def join_processes(self):
        while len(self.processes) > 0:
            self.load_config.log(LOG_LEVEL_DEBUG, 'Joining process', len(self.processes))
            old_process = self.processes.pop(0)
            old_process.join()

    # def get_data_source_summary(self):
    #     data_source_directory = self.load_config.data_source_directory()
    #
    #     summary = []
    #     for data_source_batch_name in os.listdir(data_source_directory):
    #         data_source_batch_path = os.path.join(data_source_directory, data_source_batch_name)
    #         if os.path.isfile(data_source_batch_path) and data_source_batch_name.startswith(DATA_SOURCE_BATCH_PREFIX) and 'old' not in data_source_batch_name:
    #             data_source_batch_summary = file_utils.load_file(data_source_directory, data_source_batch_name)
    #             data_source_batch_summary['batch_name'] = data_source_batch_name
    #             summary.append(data_source_batch_summary)
    #
    #     return summary

    def get_loaded_doc_count(self):
        updated_ids = []
        indexed_ids = []

        data_source_directory = self.load_config.data_source_directory()

        for data_source_batch_name in os.listdir(data_source_directory):
            data_source_batch_path = os.path.join(data_source_directory, data_source_batch_name)
            if not os.path.isfile(data_source_batch_path) and data_source_batch_name.startswith(DATA_SOURCE_BATCH_PREFIX) and 'old' not in data_source_batch_name:
                self.load_config.log(LOG_LEVEL_TRACE, 'Processing data source batch:', data_source_batch_path)

                for data_loader_batch_name in os.listdir(data_source_batch_path):
                    data_loader_batch_path = os.path.join(data_source_batch_path, data_loader_batch_name)
                    if os.path.isfile(data_loader_batch_path) and data_loader_batch_name.startswith(DATA_LOADER_BATCH_PREFIX):
                        data_loader_batch = file_utils.load_file(data_source_batch_path, data_loader_batch_name)

                        updated_ids.extend(data_loader_batch['updated_ids'])
                        indexed_ids.extend(data_loader_batch['indexed_ids'])

        return len(updated_ids) + len(indexed_ids)

    def get_data_source_batch_summary(self, data_source_batch_name):
        data_source_batch_directory = self.load_config.data_source_batch_directory(data_source_batch_name)
        data_source_directory = self.load_config.data_source_directory()

        updated_ids = []
        indexed_ids = []
        failed_ids = []
        skipped_ids = []

        for data_loader_batch_name in os.listdir(data_source_batch_directory):
            data_loader_batch_path = os.path.join(data_source_batch_directory, data_loader_batch_name)
            if os.path.isfile(data_loader_batch_path) and data_loader_batch_name.startswith(DATA_LOADER_BATCH_PREFIX):
                data_loader_batch = file_utils.load_file(data_source_batch_directory, data_loader_batch_name)

                updated_ids.extend(data_loader_batch['updated_ids'])
                indexed_ids.extend(data_loader_batch['indexed_ids'])
                failed_ids.extend(data_loader_batch['failed_ids'])
                skipped_ids.extend(data_loader_batch['skipped_ids'])

        summary = dict()
        summary['updated_ids'] = updated_ids
        summary['indexed_ids'] = indexed_ids
        summary['failed_ids'] = failed_ids
        summary['skipped_ids'] = skipped_ids

        data_source_batch_summary = file_utils.load_file(data_source_directory, data_source_batch_name + '.json')
        for key in data_source_batch_summary:
            summary[key] = data_source_batch_summary[key]

        return summary

    def get_combined_data_source_summary(self):
        total_rows = 0

        unique_ids = {}
        updated_ids = {}
        indexed_ids = {}
        failed_ids = {}
        skipped_ids = {}

        data_source_directory = self.load_config.data_source_directory()

        for data_source_batch_name in os.listdir(data_source_directory):
            data_source_batch_path = os.path.join(data_source_directory, data_source_batch_name)
            if not os.path.isfile(data_source_batch_path) and data_source_batch_name.startswith(DATA_SOURCE_BATCH_PREFIX) and 'old' not in data_source_batch_name:
                data_source_batch_summary = self.get_data_source_batch_summary(data_source_batch_name)

                total_rows += data_source_batch_summary['row_count']
                batch_unique_ids = data_source_batch_summary['unique_ids']

                batch_updated_ids = data_source_batch_summary['updated_ids']
                batch_indexed_ids = data_source_batch_summary['indexed_ids']
                batch_failed_ids = data_source_batch_summary['failed_ids']
                batch_skipped_ids = data_source_batch_summary['skipped_ids']

                # Remove duplicates across batches
                for _id in batch_unique_ids:
                    unique_ids[_id] = 0
                for _id in batch_updated_ids:
                    updated_ids[_id] = 0
                for _id in batch_indexed_ids:
                    indexed_ids[_id] = 0
                for _id in batch_failed_ids:
                    failed_ids[_id] = 0
                for _id in batch_skipped_ids:
                    skipped_ids[_id] = 0

        summary = dict()
        summary['total_rows'] = total_rows
        summary['total_ids'] = unique_ids
        summary['updated_ids'] = updated_ids
        summary['indexed_ids'] = indexed_ids
        summary['failed_ids'] = failed_ids
        summary['skipped_ids'] = skipped_ids

        return summary

    def save_summary(self):
        data_source_directory = self.load_config.data_source_directory()
        data_source_summary = self.get_combined_data_source_summary()
        file_utils.save_file(data_source_directory, 'summary.json', data_source_summary)

    def start_load_process(self, data_loader_batch, data_source_batch_name):
        progress = self.get_progress()
        self.load_config.log(LOG_LEVEL_INFO, '==================================================== Progress:', progress, '%')
        self.load_config.log(LOG_LEVEL_DEBUG, 'Creating process for', len(data_loader_batch), 'docs')
        process = Process(target=start_load, args=(self.load_config,
                                                   data_loader_batch,
                                                   data_source_batch_name))
        process.start()
        self.processes.append(process)
        if len(self.processes) >= self.load_config.process_count:
            old_process = self.processes.pop(0)
            old_process.join()

        time.sleep(self.load_config.process_spawn_delay)


def start_load(load_config, data_loader_batch, data_source_batch_name):
    data_loader = DataLoader(load_config=load_config,
                             data_loader_batch=data_loader_batch,
                             data_source_batch_name=data_source_batch_name)
    data_loader.run()



