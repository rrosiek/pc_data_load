import data_load.base.utils.file_utils as file_utils
import data_load.base.utils.data_utils as data_utils

from data_load.base.relationship_loader import RelationshipLoader

from data_load.base.utils import export_doc_ids
from data_load.base.constants import *
from data_load.irdb.config import *
import data_load.irdb.irdb_load_config as irdb_load_config


import json
import requests
import os
import time
from multiprocessing import Pool

from data_load.base.utils.logger import LOG_LEVEL_TRACE



# def get_doc_ids(load_config, index_id):
#     other_files_directory = load_config.other_files_directory()
#     file_name = 'DOC_IDS_' + index_id + '.json'

#     print 'Loading', file_name
#     doc_ids = file_utils.load_file(other_files_directory, file_name)
#     if len(doc_ids) == 0:
#         doc_ids = export_doc_ids.export_doc_ids(server=SERVER,
#                                                 src_index=INDEX_MAPPING[index_id]['index'],
#                                                 src_type=INDEX_MAPPING[index_id]['type'],
#                                                 dest_dir=other_files_directory,
#                                                 dest_file_name=file_name)

#     return doc_ids


class ExtendedRelationsLoader(object):
    def __init__(self, load_config, batch_file_name, uspto_ids):
        self.batch_file_name = batch_file_name
        self.load_config = load_config
        self.uspto_ids = uspto_ids

        # self.data_source_name = batch_file_name
        # self.source_index_id = ID_IRDB
        # self.source = 'derived'

        self.reports_directory = self.load_config.generated_files_directory()

    def load(self):
        generated_files_directory = self.load_config.generated_files_directory()
        print 'Processing batch', self.batch_file_name

        processes_ids_file_name = 'processed_ids_' + self.batch_file_name
        ids_to_update_file_name = 'ids_to_update_' + self.batch_file_name

        # Get processed ids
        processed_ids = file_utils.load_file(generated_files_directory, processes_ids_file_name)
        if processed_ids is None or len(processed_ids) == 0:
            print 'Processed ids file not found, aborting...'
            return

        # Get batch ids
        batch_ids = file_utils.load_file(generated_files_directory, self.batch_file_name)
        if batch_ids is None or len(batch_ids) == 0:
            print 'batch ids not found, aborting....'
            return

        # Continue processing
        print 'Processed ids count:', len(processed_ids), ' ~ ', len(batch_ids)

        if len(processed_ids) != len(batch_ids):
            print 'Processing not finished, aborting...'
            return
        else:
            print 'Processing complete for', self.batch_file_name, ', proceeding with data load...'

        ids_to_update = file_utils.load_file(generated_files_directory, ids_to_update_file_name)

        # Get the loaded ids
        loaded_ids = self.get_loaded_ids(self.reports_directory)

        total_count = len(ids_to_update)
        count = 0

        filtered_ids = []
        for _id in ids_to_update:
            if _id not in loaded_ids:
                filtered_ids.append(_id)

        print 'Ids to update:', len(filtered_ids)

        reformatted_array = {}
        for _id in filtered_ids:
            count += 1

            derwent_ids = ids_to_update[_id]

            uspto_ids = []
            for derwent_id in derwent_ids:
                uspto_id = derwent_id.replace('DP', '')
                if uspto_id in self.uspto_ids:
                    uspto_ids.append(uspto_id)

            if _id not in reformatted_array:
                reformatted_array[_id] = []

            if len(derwent_ids) > 0:
                relationship = {
                    'index_id': ID_DERWENT_PATENTS,
                    'ids': derwent_ids,
                    'type': RELATIONSHIP_TYPE_RELATIONS
                }

                reformatted_array[_id].append(relationship)

            if len(uspto_ids) > 0:
                relationship = {
                    'index_id': ID_USPTO,
                    'ids': uspto_ids,
                    'type': RELATIONSHIP_TYPE_RELATIONS
                }

                reformatted_array[_id].append(relationship)
                
            # print 'Reformatted ids', reformatted_array
            # time.sleep(10)
            # if len(uspto_ids) > 0:
            #     print _id, len(derwent_ids)
            #     print derwent_ids
            #     print uspto_ids
            #     time.sleep(4)

            # print extended_relations
 
        if len(reformatted_array) > 0:
            self.load_ids(reformatted_array)
            file_utils.save_file(self.reports_directory, 'loaded_ids_' + self.batch_file_name, reformatted_array.keys())

    def load_ids(self, batch):
        # print '2 Reformatted ids', len(batch)
        # for _id in batch:
        #     print batch[_id]
        #     break
        relationship_loader = RelationshipLoader(load_config=self.load_config,
                                                 data_loader_batch=batch,
                                                 _index=INDEX_MAPPING[ID_IRDB]['index'],
                                                 _type=INDEX_MAPPING[ID_IRDB]['type'],
                                                 data_source_batch_name='loaded_ids')
        # relationship_loader.test_mode = True
        relationship_loader.run()
        # self.save_load_summary(summary)


    def get_loaded_ids(self, reports_directory):
        loaded_ids = {}
        for name in os.listdir(reports_directory):
            file_path = os.path.join(reports_directory, name)
            if os.path.isfile(file_path) and name.startswith("loaded_ids_"):
                # print 'processing file:', name
                batch_data = file_utils.load_file(reports_directory, name)
                for _id in batch_data:
                    loaded_ids[_id] = 0

        return loaded_ids


class BatchRelationsLoader:
    def __init__(self, load_config):
        self.load_config = load_config
        self.pool_count = 16

    def start(self):
        generated_files_directory = self.load_config.generated_files_directory()

        batch_file_names = []
        for batch_file_name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        print "Generated ", len(batch_file_names), 'batch file names'

        p = Pool(self.pool_count)
        p.map(process_relations, batch_file_names)


def get_load_config():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DS_PATENT_RELATIONS
    # load_config.log_level = LOG_LEVEL_TRACE

    load_config.source = 'derived'
    load_config.append_relations = False

    load_config.auto_retry_load = True
    load_config.max_retries = 2
    return load_config


def process_relations(batch_file_name):
    load_config = get_load_config()
    uspto_ids = get_uspto_ids(load_config)

    extended_relations_loader = ExtendedRelationsLoader(load_config, batch_file_name, uspto_ids)
    extended_relations_loader.load()


def get_uspto_ids(load_config):
    other_files_directory = load_config.other_files_directory()
    uspto_index = INDEX_MAPPING[ID_USPTO]['index']
    uspto_type = INDEX_MAPPING[ID_USPTO]['type']
    uspto_ids = export_doc_ids.get_doc_ids(load_config.server,
                                           uspto_index,
                                           uspto_type,
                                           other_files_directory,
                                           'DOC_IDS_' + uspto_index + '.json')

    return uspto_ids


def run():
    # extended_relations_loader = ExtendedRelationsLoader('', BATCH_DATA_DIRECTORY, 1000)
    # extended_relations_loader.load()
    load_config = get_load_config()
    get_uspto_ids(load_config)

    batch_relations_loader = BatchRelationsLoader(load_config)
    batch_relations_loader.start()

import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'

# run()
