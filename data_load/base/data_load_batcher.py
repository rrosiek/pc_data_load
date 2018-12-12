import time
import psutil
import os
from multiprocessing import Process

from data_loader import DataLoader
from relationship_loader import RelationshipLoader

from utils import file_utils
from load_config import LoadConfig
from constants import DATA_LOADER_BATCH_PREFIX
from constants import DATA_SOURCE_BATCH_PREFIX
from constants import FAILED_DOCS_DIRECTORY

from data_load.base.data_loader import start_data_load
from data_load.base.relationship_loader import start_relationship_load

from load_config import *


class DataLoadBatcher(object):
    def __init__(self, load_config, _index, _type):
        self.load_config = load_config

        self.processes = []

        self.data_source_batch = {}
        self.data_source_batch_name = ''
        self.data_source_batch_directory = None

        self.index = _index
        self.type = _type

        self.load_relationships = False

        self.retry_count = 0

    def process_data_rows(self, data_source_batch_name, data_source_batch):
        self.data_source_batch = data_source_batch
        self.data_source_batch_name = data_source_batch_name
        self.data_source_batch_directory = self.load_config.data_source_batch_directory(
            data_source_batch_name)

        ids_to_load = data_source_batch.keys()

        return self.batch_process_ids(ids_to_load)

    def process_results(self, ids):
        all_ids = dict.fromkeys(ids, None)

        loaded_ids = self.get_loaded_ids(self.data_source_batch_name)
        failed_ids = self.get_failed_ids(self.data_source_batch_name)

        for _id in loaded_ids:
            if _id in all_ids:
                all_ids.pop(_id)

        for _id in failed_ids:
            if _id in all_ids:
                all_ids.pop(_id)

        failed_or_skipped_ids = []
        failed_or_skipped_ids.extend(failed_ids.keys())
        failed_or_skipped_ids.extend(all_ids.keys())

        # if len(failed_or_skipped_ids) > 0:
        #      self.load_config.log(LOG_LEVEL_ERROR, self.data_source_batch_name, len(
        #         ids), 'total docs', len(failed_or_skipped_ids), 'failed docs')
        #     if self.load_config.auto_retry_load:
        #         if self.retry_count < self.load_config.max_retries:
        #             self.retry_count += 1
        #              self.load_config.log(LOG_LEVEL_DEBUG, 'Retrying failed ids', self.retry_count)
        #             return self.retry_ids(failed_or_skipped_ids)
        #     else:
        #         input = raw_input('List failed docs?')
        #         if input.lower() in ['y', 'yes']:
        #             # List failed docs
        #             self.list_failed_docs(self.data_source_batch_directory)

        #         input = raw_input('Retry failed docs?')
        #         if input.lower() in ['y', 'yes']:
        #             return self.retry_ids(failed_or_skipped_ids)
        # else:
        #      self.load_config.log(LOG_LEVEL_INFO, '0 failed docs, continuing')

        return failed_or_skipped_ids

    def list_failed_docs(self, data_source_batch_directory):
        # print '...Processing', data_loader_batch_directory
        for name in os.listdir(data_source_batch_directory):
            file_path = os.path.join(data_source_batch_directory, name)
            if os.path.isfile(file_path) and name.startswith("failed_docs_"):
                failed_docs = file_utils.load_file(
                    data_source_batch_directory, name)
                print file_path, '- Failed docs', len(failed_docs)
                if len(failed_docs) > 0:
                    a = raw_input('List docs? (y/n)')
                    if a.lower() in ['y', 'yes']:
                        for _id in failed_docs:
                            reason = failed_docs[_id]['reason']
                            print 'Doc:', _id
                            print 'Reason', reason
                            c = raw_input('Continue?')
                            if c.lower() in ['n', 'no']:
                                break

    def rename_failed_ids_directory(self):
        # data_source_batch_name = os.path.basename(self.data_source_batch_directory)
        # data_source_directory = os.path.dirname(self.data_source_batch_directory)
        failed_docs_directory = self.load_config.failed_docs_directory(
            self.data_source_batch_name)
        failed_docs_directory_path = os.path.dirname(failed_docs_directory)
        failed_docs_directory_name = os.path.basename(failed_docs_directory)

        batch_id = str(int(round(time.time() * 1000)))
        old_failed_docs_directory = failed_docs_directory_path + \
            '/' + 'old_' + batch_id + '_' + failed_docs_directory_name
        os.rename(failed_docs_directory, old_failed_docs_directory)

        file_utils.make_directory(failed_docs_directory)

    def retry_ids(self, ids):
        # Recreate data_source_batch_directory
        self.rename_failed_ids_directory()
        return self.batch_process_ids(ids)

    def batch_process_ids(self, ids):
        print len(ids)
        batch = {}
        count = 0
        for _id in ids:
            data = self.data_source_batch.pop(_id, None)
            batch[_id] = data
            count += 1

            if count % self.load_config.data_loader_batch_size == 0:
                percentage_progress = (count / float(len(ids))) * 100
                self.load_config.log(
                    LOG_LEVEL_INFO, 'Processing ids', count, '/', len(ids), percentage_progress, '%')
                self.start_load_process(batch, self.data_source_batch_name)
                batch = {}

        if len(batch) > 0:
            self.start_load_process(batch, self.data_source_batch_name)

        self.join_processes()

        # Analyse results
        return self.process_results(ids)

    def join_processes(self):
        while len(self.processes) > 0:
            self.load_config.log(
                LOG_LEVEL_DEBUG, 'Joining process', len(self.processes))
            old_process = self.processes.pop(0)
            old_process.join()

    def get_loaded_ids(self, data_source_batch_name):
        loaded_docs_directory = self.load_config.loaded_docs_directory(
            data_source_batch_name)

        loaded_ids = {}
        for name in os.listdir(loaded_docs_directory):
            file_path = os.path.join(loaded_docs_directory, name)
            if os.path.isfile(file_path) and name.startswith(DATA_LOADER_BATCH_PREFIX):
                self.load_config.log(
                    LOG_LEVEL_TRACE, 'processing file:', file_path)
                batch_data = file_utils.load_file(loaded_docs_directory, name)
                updated_ids = batch_data['updated_ids']
                indexed_ids = batch_data['indexed_ids']

                for _id in updated_ids:
                    loaded_ids[_id] = 0

                for _id in indexed_ids:
                    loaded_ids[_id] = 0

        self.load_config.log(LOG_LEVEL_DEBUG, 'Loaded ids', len(loaded_ids))

        return loaded_ids

    def get_failed_ids(self, data_source_batch_name):
        failed_docs_directory = self.load_config.failed_docs_directory(
            data_source_batch_name)

        failed_ids = {}
        for name in os.listdir(failed_docs_directory):
            file_path = os.path.join(failed_docs_directory, name)
            if os.path.isfile(file_path) and name.startswith(DATA_LOADER_BATCH_PREFIX):
                self.load_config.log(
                    LOG_LEVEL_TRACE, 'processing file:', file_path)
                batch_failed_docs = file_utils.load_file(
                    failed_docs_directory, name)

                for _id in batch_failed_docs:
                    failed_ids[_id] = 0

        self.load_config.log(LOG_LEVEL_DEBUG, 'Failed ids', len(failed_ids))

        return failed_ids

    def is_memory_available(self):
        vm = psutil.virtual_memory()
        total_memory = vm.total

        required = (0.1875 * total_memory)
        required_in_percentage = (required / float(total_memory)) * 100

        max_memory_percent = 100 - required_in_percentage

        self.load_config.log(LOG_LEVEL_INFO, 'Processes:', len(
            self.processes), ", Memory Used:", self.get_memory_percent(), ", Memory Max:", max_memory_percent)

        if self.get_memory_percent() <= max_memory_percent:
            return True

        return False

    def get_memory_percent(self):
        memory_details = psutil.virtual_memory()
        memory_percent = memory_details.percent
        return memory_percent

    def join_proceses(self):
        if len(self.processes) > 0:
            old_process = self.processes.pop(0)
            old_process.join()

    def start_load_process(self, data_loader_batch, data_source_batch_name):
        while not self.is_memory_available():
            self.load_config.log(
                LOG_LEVEL_INFO, 'data load batcher waiting...')
            # time.sleep(3)
            self.join_proceses()

        if len(self.processes) >= self.load_config.process_count:
            self.join_proceses()

        self.load_config.log(LOG_LEVEL_DEBUG, 'Process count:' + str(self.load_config.process_count))
        self.load_config.log(LOG_LEVEL_INFO, 'Creating process for', len(data_loader_batch), 'docs')

        process = None

        if self.load_relationships:
            if self.load_config.process_count == 1:
                start_relationship_load(self.load_config,
                                data_loader_batch,
                                self.index,
                                self.type,
                                data_source_batch_name)
            else:
                process = Process(target=start_relationship_load, args=(self.load_config,
                                                                        data_loader_batch,
                                                                        self.index,
                                                                        self.type,
                                                                        data_source_batch_name))
        else:
            if self.load_config.process_count == 1:
                start_data_load(self.load_config,
                                data_loader_batch,
                                self.index,
                                self.type,
                                data_source_batch_name)
            else:
                process = Process(target=start_data_load, args=(self.load_config,
                                                                data_loader_batch,
                                                                self.index,
                                                                self.type,
                                                                data_source_batch_name))

        if process is not None:
            process.start()
            self.processes.append(process)

            time.sleep(self.load_config.process_spawn_delay)


# def start_relationship_load(load_config, data_loader_batch, _index, _type, data_source_batch_name):
#     relationship_loader = RelationshipLoader(load_config=load_config,
#                                              data_loader_batch=data_loader_batch,
#                                              _index=_index,
#                                              _type=_type,
#                                              data_source_batch_name=data_source_batch_name)
#     relationship_loader.run()


# def start_load(load_config, data_loader_batch, _index, _type, data_source_batch_name):
#     data_loader = DataLoader(load_config=load_config,
#                              data_loader_batch=data_loader_batch,
#                              _index=_index,
#                              _type=_type,
#                              data_source_batch_name=data_source_batch_name)
#     data_loader.run()
