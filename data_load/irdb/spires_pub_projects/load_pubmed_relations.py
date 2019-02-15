from data_load.irdb.config import *
import data_load.irdb.irdb_load_config as irdb_load_config
import data_load.irdb.spires_pub_projects.pubmed_load_config as pubmed_load_config
import data_load.base.utils.file_utils as file_utils

from data_load.base.constants import ID_IRDB, ID_PUBMED, RELATIONSHIP_TYPE_CITATIONS, INDEX_MAPPING

from data_load.base.data_load_batcher import DataLoadBatcher

from data_load.pubmed2018.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2018.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig

import os

class LoadRelationships(object):

    def __init__(self):
        self.temp = {}

    def get_load_config(self):
        load_config = pubmed_load_config.get_load_config()
        load_config.data_source_name = 'spires_pubmed_relations'
        # load_config.log_level = LOG_LEVEL_TRACE

        load_config.source = 'irdb'
        load_config.append_relations = False

        load_config.auto_retry_load = True
        load_config.max_retries = 2
        return load_config


    def split_to_batches(self):
        load_config = self.get_load_config()
        other_files_directory = load_config.other_files_directory()

        # appl_id__pmid__mapping = file_utils.unpickle_file(other_files_directory, 'appl_id__pmid__mapping.json')
        pmid__appl_id__mapping = file_utils.unpickle_file(other_files_directory, 'pmid__appl_id__mapping.json')

        pubmed_ids = {}
        pubmed_ids = load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                                      relations_array=pmid__appl_id__mapping,
                                                      dest_index_id=ID_IRDB,
                                                      relationship_type=RELATIONSHIP_TYPE_CITATIONS)

        generated_files_directory = load_config.data_source_directory()
        pubmed_id_keys = pubmed_ids.keys()
        pubmed_id_keys.sort()

        max_batch_count = 10000

        batch_index = 0
        batch_ids = {}
        batch_file_names = []
        # Splitting into batches
        for _id in pubmed_id_keys:
            batch_ids[_id] = pubmed_ids[_id]

            if len(batch_ids) >= max_batch_count:
                print 'Writing batch:', batch_index
                batch_file_name = 'batch_' + str(batch_index) + '.json'
                batch_file_names.append(batch_file_name)
                file_utils.save_file(generated_files_directory, batch_file_name, batch_ids)

                batch_ids = {}
                batch_index += 1 

        return batch_file_names

    def run(self):
        self.process_batches()

    def process_batch(self, batch):
        load_config = self.get_load_config()

        data_load_batcher = DataLoadBatcher(load_config, load_config.index, load_config.type)
        data_load_batcher.load_relationships = True
        failed_ids = data_load_batcher.process_data_rows('pubmed_relations', batch)

        print len(failed_ids), 'failed ids'

    def process_batches(self):
        load_config = self.get_load_config()
        generated_files_directory = load_config.data_source_directory()
        other_files_directory = load_config.other_files_directory()

        batch_file_names = []
        for batch_file_name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        print "Generated ", len(batch_file_names), 'batch file names'

        batch_file_names.sort()

        if len(batch_file_names) == 0:
            batch_file_names = self.split_to_batches()


        processed_batches = file_utils.load_file(other_files_directory, 'processed_spires_pubmed_batches.json')
        for batch_file_name in batch_file_names:
            if batch_file_name not in processed_batches:
                print 'Loading batch', batch_file_name
                batch = file_utils.load_file(generated_files_directory, batch_file_name)
                self.process_batch(batch)
                processed_batches[batch_file_name] = 0
                file_utils.save_file(other_files_directory, 'processed_spires_pubmed_batches.json', processed_batches)



def run():
    load_relationships = LoadRelationships()
    load_relationships.run()


import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'
    
# run()