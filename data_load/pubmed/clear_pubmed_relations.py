
from data_load.base.utils import clear_relationships
from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.utils.data_utils  import DataUtils
from data_load.base.utils.batch_doc_processor import BatchDocProcessor

import data_load.base.utils.file_utils as file_utils
from data_load.base.utils import export_doc_ids
import requests
import json
import time
import os

from data_load.base.constants import RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS, ID_PUBMED 


TEMP_DIR = '/data/data_loading/pubmed_2019/pubmed2019/temp_files/clear_relations'

class ClearPubmedRelations(object):

    def __init__(self, load_config):
        self.load_config = load_config

        file_utils.make_directory(TEMP_DIR)

    def run(self):
        self.process_batches()

    def process_batches(self):
        batch_file_names = []
        for batch_file_name in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        print "Generated ", len(batch_file_names), 'batch file names'

        batch_file_names.sort()

        if len(batch_file_names) == 0:
            batch_file_names = self.split_to_batches()

        print len(batch_file_names)
        raw_input('Continue?')

        processed_batches = file_utils.load_file(TEMP_DIR, 'processed_pubmed2019_docs_with_relations_batches.json')
        for batch_file_name in batch_file_names:
            if batch_file_name not in processed_batches:
                print 'Loading batch', batch_file_name
                batch = file_utils.load_file(TEMP_DIR, batch_file_name)
                self.process_docs_batch(batch)
                processed_batches[batch_file_name] = 0
                file_utils.save_file(TEMP_DIR, 'processed_pubmed2019_docs_with_relations_batches.json', processed_batches)

    def split_to_batches(self):
        print 'Fetching doc ids for', self.load_config.index, self.load_config.type
        query = {
            "bool": {
            "should": [
                {
                "bool": {
                    "must": [
                    {
                        "match": {
                        "citations.index_id": "PUBMED"
                        }
                    },
                    {
                        "match": {
                        "citations.source": ""
                        }
                    }
                    ]
                }
                },
                {
                "bool": {
                    "must": [
                    {
                        "match": {
                        "cited_bys.index_id": "PUBMED"
                        }
                    },
                    {
                        "match": {
                        "cited_bys.source": ""
                        }
                    }
                    ]
                }
                }
            ]
            }
        }

        all_pubmed_ids = export_doc_ids.get_doc_ids(self.load_config.server,
                                                    self.load_config.index,
                                                    self.load_config.type,
                                                    self.load_config.other_files_directory(),
                                                    'DOCS_WITH_RELATIONS_' + self.load_config.index + '.json', query=query)
        all_pubmed_ids = all_pubmed_ids.keys()
        all_pubmed_ids.sort()

        max_batch_count = 10000
        
        batch_file_names = []
        batch_index = 0
        batch_ids = []
        # Splitting into batches
        for _id in all_pubmed_ids:
            batch_ids.append(_id)

            if len(batch_ids) >= max_batch_count:
                print 'Writing batch:', batch_index
                batch_file_name = 'batch_' + str(batch_index) + '.json'
                batch_file_names.append(batch_file_name)
                file_utils.save_file(TEMP_DIR, batch_file_name, batch_ids)

                batch_ids = []
                batch_index += 1

        if len(batch_ids) > 0:
            print 'Writing batch:', batch_index
            batch_file_name = 'batch_' + str(batch_index) + '.json'
            batch_file_names.append(batch_file_name)
            file_utils.save_file(TEMP_DIR, batch_file_name, batch_ids)

            batch_index += 1

        return batch_file_names

    def process_docs_batch(self, doc_ids):
        print 'Clearing relations'
      
        source = ''
        clear_relationships.batch_clear_relations_for_ids(server=self.load_config.server,
                                                        _ids=doc_ids,
                                                        src_index=self.load_config.index,
                                                        src_type=self.load_config.type,
                                                        source=source,
                                                        dest_index_ids=[ID_PUBMED],
                                                        relationship_types=[RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS])

    
# def run(load_config):
#     other_files_directory = load_config.other_files_directory()

#     pubmed_index = load_config.index
#     pubmed_type = load_config.type

#     query = {
#         "bool": {
#         "should": [
#             {
#             "bool": {
#                 "must": [
#                 {
#                     "match": {
#                     "citations.index_id": "PUBMED"
#                     }
#                 },
#                 {
#                     "match": {
#                     "citations.source": ""
#                     }
#                 }
#                 ]
#             }
#             },
#             {
#             "bool": {
#                 "must": [
#                 {
#                     "match": {
#                     "cited_bys.index_id": "PUBMED"
#                     }
#                 },
#                 {
#                     "match": {
#                     "cited_bys.source": ""
#                     }
#                 }
#                 ]
#             }
#             }
#         ]
#         }
#     }

#     all_pubmed_ids = export_doc_ids.get_doc_ids(load_config.server,
#                                                 pubmed_index,
#                                                 pubmed_type,
#                                                 other_files_directory,
#                                                 'DOCS_WITH_RELATIONS_' + pubmed_index + '.json', query=query)
#     all_pubmed_ids = all_pubmed_ids.keys()
#     all_pubmed_ids.sort()

#     print len(all_pubmed_ids), 'pubmed ids'

#     raw_input('Continue?')

#     source = ''



#     clear_relationships.batch_clear_relations_for_ids(server=load_config.server,
#                                                       _ids=all_pubmed_ids,
#                                                       src_index=pubmed_index,
#                                                       src_type=pubmed_type,
#                                                       source=source,
#                                                       dest_index_ids=[ID_PUBMED],
#                                                       relationship_types=[RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS])

import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'
    
# run()
