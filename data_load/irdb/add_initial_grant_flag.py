import irdb_load_config

import data_load.base.utils.file_utils as file_utils
from data_load.base.constants import *
from data_load.base.utils.process_index import ProcessIndex
from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.utils.export_doc_ids import get_doc_ids

from  data_load.base.utils.data_utils import DataUtils

from config import *
import json


class AddInitialGrantFlag(object):

    def __init__(self, load_config):
        self.load_config = load_config
        self.grant_num_groups = {}

        self.data_loader_utils = DataLoaderUtils(SERVER, INDEX, TYPE)

        self.bulk_data_size = 300000
        self.docs_processed = 0
        self.total_doc_count = 0

    def process_doc(self, _id, doc):
        if 'grant_num' in doc:
            grant_num = doc['grant_num']

            grant_num_comps = grant_num.split('-')
            if grant_num_comps[0] not in self.grant_num_groups:
                self.grant_num_groups[grant_num_comps[0]] = {}

            fy = None

            if 'fy' in doc:
                fy = doc['fy']
                if len(fy) > 0:
                    fy = int(fy)
                else:
                    fy = None

            if fy is not None:
                self.grant_num_groups[grant_num_comps[0]][_id] = {
                    'id': _id,
                    'fy': fy,
                    'grant_num': grant_num
                }       
            
        updated_doc = {}
        updated_doc['initial_grant'] = False

        return updated_doc

    def process_grant_num_groups(self):

        print 'Processing', len(self.grant_num_groups), 'grant_num groups'
        bulk_data = ''

        total_grant_num_groups = len(self.grant_num_groups)
        count = 0

        for grant_num in self.grant_num_groups:
            count += 1
            progress = ((count / float(total_grant_num_groups)) * 100)
            print 'Pass 2: progress', count, '/', total_grant_num_groups, progress, '%'

            grant_num_group = self.grant_num_groups[grant_num]

            # Find doc with lowest fy
            lowest_item = None
            for _id in grant_num_group:
                fy_data = grant_num_group[_id]
              
                if lowest_item is None:
                    lowest_item = fy_data
                else:
                    fy = fy_data['fy']
                    lowest_item_fy = lowest_item['fy']
                    if fy < lowest_item_fy:
                        lowest_item = fy_data
            
            if lowest_item is not None:
                _id = lowest_item['id']

                doc = {}
                doc['initial_grant'] = True

                bulk_data += self.data_loader_utils.bulk_update_header(_id)
                bulk_data += '\n'
                doc = {
                    'doc': doc
                }
                bulk_data += json.dumps(doc)
                bulk_data += '\n'
       
            if len(bulk_data) >= self.bulk_data_size:
                self.load_bulk_data(bulk_data)
                bulk_data = ''

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)

    def load_bulk_data(self, bulk_data):
        self.data_loader_utils.load_bulk_data(bulk_data)
        # pass

    def run(self):
        doc_ids = get_doc_ids(server=self.load_config.server,
                                src_index=self.load_config.index,
                                src_type=self.load_config.type,
                                dest_dir=self.load_config.other_files_directory(),
                                dest_file_name="ALL_IRDB_IDS.json")

        doc_ids = doc_ids.keys()

        self.total_doc_count = len(doc_ids)

        data_utils = DataUtils()
        data_utils.batch_fetch_docs_for_ids(base_url=self.load_config.server,
                                            ids=doc_ids,
                                            index=self.load_config.index,
                                            type=self.load_config.type,
                                            docs_fetched=self.docs_fetched)

        self.process_grant_num_groups()

    def docs_fetched(self, docs, index, type):
        docs_to_process = {}

        self.docs_processed += len(docs)
        progress = ((self.docs_processed / float(self.total_doc_count)) * 100)
        print 'Pass 1: progress', self.docs_processed, '/', self.total_doc_count, progress, '%'

        # print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                docs_to_process[_id] = existing_doc

        self.process_docs(docs_to_process) 

        # print 'Processed docs', self.processed_docs, 'Pubmed relations', len(self.pubmed_relations) 

    def process_docs(self, docs):
        bulk_data = ''

        for _id in docs:
            doc = docs[_id]

            processed_doc = self.process_doc(_id, doc)
            
            if processed_doc is not None:
                bulk_data += self.data_loader_utils.bulk_update_header(_id)
                bulk_data += '\n'
                updated_doc = {
                    'doc': processed_doc
                }
                bulk_data += json.dumps(updated_doc)
                bulk_data += '\n'
       
            if len(bulk_data) >= self.bulk_data_size:
                # print 'loading bulk data...'
                self.load_bulk_data(bulk_data)
                bulk_data = ''

        if len(bulk_data) > 0:
            # print 'loading bulk data...'
            self.load_bulk_data(bulk_data)


def run(load_config):
    add_initial_grant_flag = AddInitialGrantFlag(load_config)
    add_initial_grant_flag.run()


import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'
    
# run()