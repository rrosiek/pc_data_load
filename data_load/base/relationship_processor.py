import os

from multiprocessing import Process
from data_processor import DataProcessor

from relationship_loader import RelationshipLoader

from constants import DATA_SOURCE_BATCH_PREFIX
from utils import file_utils

from load_config import *

class RelationshipProcessor(DataProcessor):
    def __init__(self, load_config, data_source):
        super(RelationshipProcessor, self).__init__(load_config, data_source)

    def process_data_rows(self, data_source_batch_name):
        pass

    # def data_source_batch_name(self, start_index, row_count):
    #     data_source_batch_name = DATA_SOURCE_BATCH_PREFIX + '_' + str(start_index) + '_' + str(row_count)
    #     return data_source_batch_name

    def process_relations_rows(self, data_rows, data_source_batch_name, source_index_id):
        data_source_directory = self.load_config.data_source_directory()
        data_source_batch_directory = self.load_config.data_source_batch_directory(data_source_batch_name)
        data_source_batch_directory_for_source = data_source_batch_directory + '/' + source_index_id
        file_utils.make_directory(data_source_batch_directory_for_source)

        filtered_ids = []
        if self.mode == DataProcessor.MODE_RETRY_FAILED_DOCS or self.mode == DataProcessor.MODE_NORMAL_LOAD:
            loaded_ids = self.get_loaded_ids(data_source_batch_directory_for_source)
        else:
            loaded_ids = {}

        # filter ids
        for _id in data_rows:
            if _id not in loaded_ids:
                filtered_ids.append(_id)

        self.load_config.log(LOG_LEVEL_INFO,'source index', source_index_id)
        self.load_config.log(LOG_LEVEL_INFO, 'loaded ids', len(loaded_ids))
        self.load_config.log(LOG_LEVEL_INFO, 'ids to load', len(filtered_ids))

        if self.mode is not DataProcessor.MODE_NORMAL_LOAD:
            batch_id = str(int(round(time.time() * 1000)))
            old_data_source_batch_directory = data_source_directory + '/' + 'old_' + data_source_batch_name + '_' + batch_id
            os.rename(data_source_batch_directory, old_data_source_batch_directory)

        batch = {}
        count = 0
        for _id in filtered_ids:
            data = data_rows.pop(_id, None)
            batch[_id] = data
            count += 1
            # if count % 1000 == 0:
            #     print 'Adding id to batch', _id

            if count % self.load_config.data_loader_batch_size == 0:
                self.start_relationship_load_process(batch, data_source_batch_name, source_index_id)
                batch = {}

        if len(batch) > 0:
            self.start_relationship_load_process(batch, data_source_batch_name, source_index_id)

        self.join_processes()

    def reformat(self, reformatted_array, relations_array, dest_index_id, relationship_type):
        for _id in relations_array:
            if _id not in reformatted_array:
                reformatted_array[_id] = []

            relationship = {
                'index_id': dest_index_id,
                'ids': relations_array[_id],
                'type': relationship_type
            }

            reformatted_array[_id].append(relationship)

        return reformatted_array

    def start_relationship_load_process(self, data_loader_batch, data_source_batch_name, source_index_id):
        self.load_config.log(LOG_LEVEL_INFO, 'Creating process for', len(data_loader_batch), 'docs')
        process = Process(target=start_relationship_load, args=(self.load_config,
                                                                data_loader_batch,
                                                                data_source_batch_name,
                                                                source_index_id))
        process.start()
        self.processes.append(process)
        if len(self.processes) > self.load_config.process_count:
            old_process = self.processes.pop(0)
            old_process.join()

        time.sleep(self.load_config.process_spawn_delay)


def start_relationship_load(load_config, data_loader_batch, data_source_batch_name, source_index_id):
    relationship_loader = RelationshipLoader(load_config=load_config,
                                             data_loader_batch=data_loader_batch,
                                             source_index_id=source_index_id,
                                             data_source_batch_name=data_source_batch_name)
    relationship_loader.run()
