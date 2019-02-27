import data_load.base.utils.file_utils as file_utils
from data_load.base.utils.data_utils import DataUtils

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


def get_doc_ids(load_config, index_id):
    other_files_directory = load_config.other_files_directory()
    file_name = 'DOC_IDS_' + index_id + '.json'

    print 'Loading', file_name
    doc_ids = file_utils.load_file(other_files_directory, file_name)
    if len(doc_ids) == 0:
        doc_ids = export_doc_ids.export_doc_ids(server=SERVER,
                                                src_index=INDEX_MAPPING[index_id]['index'],
                                                src_type=INDEX_MAPPING[index_id]['type'],
                                                dest_dir=other_files_directory,
                                                dest_file_name=file_name)

    return doc_ids

def get_load_config():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DATA_SOURCE_NAME
    # load_config.log_level = LOG_LEVEL_TRACE

    load_config.source = 'derived'
    load_config.append_relations = False

    load_config.auto_retry_load = True
    load_config.max_retries = 2
    return load_config

class Verify(object):
    def __init__(self, load_config):
        self.uspto_ids = get_doc_ids(load_config, ID_USPTO)
        self.load_config = load_config
        self.reports_directory = load_config.generated_files_directory()

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

    def process_batch(self, load_config, batch_file_name):
        generated_files_directory = load_config.generated_files_directory()
    
        ids_to_update_file_name = 'ids_to_update_' + batch_file_name
        ids_to_update = file_utils.load_file(generated_files_directory, ids_to_update_file_name)

        return ids_to_update

    def run(self):
        load_config = get_load_config()
        generated_files_directory = load_config.generated_files_directory()

        batch_file_names = []
        for batch_file_name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        print "Generated ", len(batch_file_names), 'batch file names'

        total_ids_to_update = 0
        uspto_relations = {}
        derwent_relations = {}
        for batch_file_name in batch_file_names:
            ids_to_update = self.process_batch(load_config, batch_file_name)

            for _id in ids_to_update:   
                derwent_ids = ids_to_update[_id]

                uspto_ids = []
                for derwent_id in derwent_ids:
                    uspto_id = derwent_id.replace('DP', '')
                    if uspto_id in self.uspto_ids:
                        uspto_ids.append(uspto_id)

                if len(derwent_ids) > 0:
                    derwent_relations[_id] = 0

                if len(uspto_ids) > 0:
                    uspto_relations[_id] = 0

            total_ids_to_update += len(ids_to_update)

        print len(uspto_relations), 'uspto_relations'
        print len(derwent_relations), 'derwent_relations'

def start():
    load_config = get_load_config()
    reports_directory = load_config.generated_files_directory()
    verify_ids = Verify(load_config)
    verify_ids.run()

    # print len(loaded_ids), 'loaded_ids'


start()
