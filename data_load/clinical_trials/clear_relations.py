import ct_load_config
from data_load.base.utils import clear_relationships

import data_load.base.utils.file_utils as file_utils
from data_load.base.constants import *
from data_load.base.utils import export_doc_ids

import os

from config import *


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

    
def run(load_config, pubmed_index, pubmed_type):
    # load_config = ct_load_config.get_load_config()
    load_config.data_source_name = 'ct_references'
    other_files_directory = load_config.other_files_directory()

    # pubmed_index = INDEX_MAPPING[ID_PUBMED]['index']
    # pubmed_type = INDEX_MAPPING[ID_PUBMED]['type']

    all_pubmed_ids = export_doc_ids.get_doc_ids(load_config.server,
                                                pubmed_index,
                                                pubmed_type,
                                                other_files_directory,
                                                'DOC_IDS_' + pubmed_index + '.json')
    all_pubmed_ids = all_pubmed_ids.keys()

    print len(all_pubmed_ids), 'pubmed ids'

    source = 'ct_references'
    clear_relationships.batch_clear_relations_for_ids(server=load_config.server,
                                                      _ids=all_pubmed_ids,
                                                      src_index=pubmed_index,
                                                      src_type=pubmed_type,
                                                      source=source,
                                                      dest_index_ids=[ID_CLINICAL_TRIALS],
                                                      relationship_types=[RELATIONSHIP_TYPE_CITED_BYS])

    source = 'pardi'
    clear_relationships.batch_clear_relations_for_ids(server=load_config.server,
                                                      _ids=all_pubmed_ids,
                                                      src_index=pubmed_index,
                                                      src_type=pubmed_type,
                                                      source=source,
                                                      dest_index_ids=[ID_CLINICAL_TRIALS],
                                                      relationship_types=[RELATIONSHIP_TYPE_CITATIONS])

# import sys
# import time

# def run_test():
#     print __file__, 'running....'
#     time.sleep(3)
#     print __file__, 'Done'
    
# run()
