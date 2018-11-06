
from data_load.base.utils.copy_docs import CopyDocs
import data_load.base.utils.export_doc_ids as export_doc_ids
from data_load.base.utils.batch_doc_processor import BatchDocProcessor
from data_load.base.utils.data_loader_utils import DataLoaderUtils

import data_load.base.utils.file_utils as file_utils
import data_load.base.utils.data_utils as data_utils

import json



class ProcessIndex(object):

    def __init__(self, server, src_index, src_type, process_doc_method):
        self.server = server
        self.index = src_index
        self.type = src_type
        self.process_doc_method = process_doc_method

        self.batch_size = 5000
        self.process_count = 2
        self.process_spawn_delay = 0.15
        self.bulk_data_size = 300000

        self.data_loader_utils = DataLoaderUtils(self.server,
                                                 self.index,
                                                 self.type)

    def run(self):
        # doc_ids = export_doc_ids( self.server, self.index,
        #                             self.type, self.index + '_' + self.type , 'doc_ids.json')

        doc_ids = file_utils.load_file(self.index, self.index + '_ids.json')

        if len(doc_ids) == 0:
            doc_ids = export_doc_ids.export_doc_ids(self.server, self.index, self.type)

        doc_ids = doc_ids.keys()

        batch_doc_processor = BatchDocProcessor(doc_ids, self.process_batch, self.batch_size, self.process_count, self.process_spawn_delay)
        batch_doc_processor.run()

    def docs_fetched(self, docs, index, type):
        docs_to_process = {}

        print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                docs_to_process[_id] = existing_doc

        self.process_docs(docs_to_process)

    def process_docs(self, docs):
        bulk_data = ''

        for _id in docs:
            doc = docs[_id]

            processed_doc = self.process_doc_method(_id, doc)
            
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

    def load_bulk_data(self, bulk_data):
        self.data_loader_utils.load_bulk_data(bulk_data)
        # pass

    def process_batch(self, doc_ids):
        data_utils.batch_fetch_docs_for_ids(base_url=self.server,
                                            ids=doc_ids,
                                            index=self.index,
                                            type=self.type,
                                            docs_fetched=self.docs_fetched)


