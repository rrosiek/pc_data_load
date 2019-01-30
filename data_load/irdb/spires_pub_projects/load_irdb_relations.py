from data_load.irdb.config import *
# import data_load.irdb.irdb_load_config as irdb_load_config

import data_load.base.utils.file_utils as file_utils

from data_load.base.constants import ID_IRDB, ID_PUBMED, RELATIONSHIP_TYPE_CITED_BYS

from data_load.base.data_load_batcher import DataLoadBatcher


class LoadRelationships(object):

    def __init__(self):
        self.temp = {}

    def get_load_config(self):
        load_config = irdb_load_config.get_load_config()
        load_config.data_source_name = DS_SPIRES_PUB_PROJECTS
        # load_config.log_level = LOG_LEVEL_TRACE

        load_config.source = 'irdb'
        load_config.append_relations = False

        load_config.auto_retry_load = True
        load_config.max_retries = 2
        return load_config

    def run(self):
        load_config = self.get_load_config()
        other_files_directory = load_config.other_files_directory()

        appl_id__pmid__mapping = file_utils.unpickle_file(other_files_directory, 'appl_id__pmid__mapping.json')
        # pmid__appl_id__mapping = file_utils.unpickle_file(other_files_directory, 'pmid__appl_id__mapping.json')

        irdb_ids = {}
        irdb_ids = load_config.data_mapper.reformat(reformatted_array=irdb_ids,
                                                      relations_array=appl_id__pmid__mapping,
                                                      dest_index_id=ID_PUBMED,
                                                      relationship_type=RELATIONSHIP_TYPE_CITED_BYS)
        count = 0 
        for irdb_id in irdb_ids:
            data = irdb_ids[irdb_id]
            print irdb_id, data
            count += 1
            if count == 3:
                break

        # raw_input('Continue?')

        data_load_batcher = DataLoadBatcher(load_config, load_config.index, load_config.type)
        data_load_batcher.load_relationships = True
        failed_ids = data_load_batcher.process_data_rows('irdb_relations', irdb_ids)

        print len(failed_ids), 'failed ids'


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