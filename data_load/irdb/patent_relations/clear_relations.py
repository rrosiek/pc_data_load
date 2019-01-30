from data_load.base.utils import clear_relationships

import data_load.base.utils.file_utils as file_utils
from data_load.base.constants import *
from data_load.base.utils import export_doc_ids

from data_load.irdb.config import *
import data_load.irdb.irdb_load_config as irdb_load_config

import time

import os


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

    
def run():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DS_PATENT_RELATIONS

    all_irdb_ids = export_doc_ids.get_doc_ids_for_load_config(load_config)
    all_irdb_ids = all_irdb_ids.keys()

    print 'IRDB IDS', len(all_irdb_ids)
    time.sleep(10)

    source = 'derived'
    clear_relationships.batch_clear_relations_for_ids(server=SERVER,
                                                      _ids=all_irdb_ids,
                                                      src_index=INDEX_MAPPING[ID_IRDB]['index'],
                                                      src_type=INDEX_MAPPING[ID_IRDB]['type'],
                                                      source=source,
                                                      dest_index_ids=[ID_USPTO, ID_DERWENT_PATENTS],
                                                      relationship_types=[RELATIONSHIP_TYPE_RELATIONS])

import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'

# run()