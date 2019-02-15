from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.utils.data_utils  import DataUtils
from data_load.base.utils.batch_doc_processor import BatchDocProcessor

import requests
import json
import time
import os
import data_load.base.utils.file_utils as file_utils
from data_load.base.utils import export_doc_ids

from data_load.base.constants import ID_PUBMED

TEMP_DIR = '/data/data_loading/pubmed_2019/pubmed2019/temp_files/copy_grants'

class CopyGrants(object):

    def __init__(self, src_server, dest_server, src_index, src_type, dst_index, dst_type, username, password):
        self.src_data_loader_utils = DataLoaderUtils(src_server, src_index, src_type)
        self.dest_data_loader_utils = DataLoaderUtils(dest_server, dst_index, dst_type)

        self.data_utils = DataUtils()

        self.username = username
        self.password = password

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

        processed_batches = file_utils.load_file(TEMP_DIR, 'processed_pubmed2018_docs_with_grants_batches.json')
        for batch_file_name in batch_file_names:
            if batch_file_name not in processed_batches:
                print 'Loading batch', batch_file_name
                batch = file_utils.load_file(TEMP_DIR, batch_file_name)
                self.copy_docs_batch(batch)
                processed_batches[batch_file_name] = 0
                file_utils.save_file(TEMP_DIR, 'processed_pubmed2018_docs_with_grants_batches.json', processed_batches)

    def split_to_batches(self):
        server = self.src_data_loader_utils.server
        src_index = self.src_data_loader_utils.index
        src_type = self.src_data_loader_utils.type

        print 'Fetching doc ids for', src_index, src_type
        query = {
            "nested": {
                "path": "grants",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "exists": {
                                    "field": "grants"
                                }
                            }
                        ]
                    }
                }
            }
        }

        all_pubmed_ids = export_doc_ids.get_doc_ids(server,
                                                    src_index,
                                                    src_type,
                                                    TEMP_DIR,
                                                    'pubmed2018_docs_with_grants.json', query=query)
        # all_pubmed_ids = all_pubmed_ids.keys()
        # all_pubmed_ids.sort()
        self.total_doc_count = len(all_pubmed_ids)  

        max_batch_count = 5000
        
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

    def copy_docs_batch(self, doc_ids):
        print 'Fetching docs'
        self.data_utils.batch_fetch_docs_for_ids(base_url=self.src_data_loader_utils.server,
                                                ids=doc_ids,
                                                index=self.src_data_loader_utils.index,
                                                type=self.src_data_loader_utils.type,
                                                docs_fetched=self.docs_fetched,
                                                batch_size=500)
   
    def docs_fetched(self, docs, index, type):
        print 'Docs fetched', len(docs)
        docs_to_copy = {}

        # print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                docs_to_copy[_id] = existing_doc

        self.copy_relations(docs_to_copy)
    
    def load_bulk_data(self, bulk_data):
        print 'Bulk data size', len(bulk_data), 'loading...'
        response = self.dest_data_loader_utils.load_bulk_data(bulk_data)

        if response:
            pass
            # print 'Done loading bulk data, saving response'
        else:
            print 'Bulk data load failed'

    def copy_relations(self, src_docs):
        bulk_data = ''
        count = 0

        # Copy relations 
        for _id in src_docs:
            src_doc = src_docs[_id]
            
            doc = {}
            if 'grants' in src_doc:
                doc['grants'] = src_doc['grants']

            count += 1

            if len(doc) > 0: 
                bulk_data += self.dest_data_loader_utils.bulk_update_header(_id)
                bulk_data += '\n'
                doc = {
                    'doc': doc
                }
                bulk_data += json.dumps(doc)
                bulk_data += '\n'

                # if count % 1000 == 0:
                #     print 'Processed', 1000, 'docs'
                if len(bulk_data) >= 150000:
                    print _id
                    self.load_bulk_data(bulk_data)
                    # print 'Copied', count, 'docs'
                    bulk_data = ''

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)
            pass


src_server = 'http://localhost:9200'
src_index = 'pubmed2018_v5'
src_type = 'article'

dest_server = 'http://localhost:9200'
dest_index = 'pubmed2019'
dest_type = 'article'

copy_grants = CopyGrants(src_server=src_server, 
                        dest_server=dest_server, 
                        src_index=src_index, 
                        src_type=src_type, 
                        dst_index=dest_index, 
                        dst_type=dest_type, 
                        username='', 
                        password='')
copy_grants.run()
