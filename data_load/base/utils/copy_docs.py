from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.utils.data_utils  import DataUtils
from data_load.base.utils.batch_doc_processor import BatchDocProcessor

import requests
import json
import data_load.base.utils.file_utils
from data_load.migrate_indices import migrate_index


class CopyDocs(object):

    def __init__(self, src_server, dest_server, src_index, src_type, dst_index, dst_type):
        self.src_data_loader_utils = DataLoaderUtils(src_server, src_index, src_type)
        self.dest_data_loader_utils = DataLoaderUtils(dest_server, dst_index, dst_type)

        self.processed_doc_count = 0
        self.total_doc_count = 0

        self.data_utils = DataUtils()

    def get_total_doc_count(self):
        return self.data_utils.get_total_doc_count(base_url=self.src_data_loader_utils.server,
                                            index=self.src_data_loader_utils.index,
                                            type=self.src_data_loader_utils.type)

    def docs_fetched(self, docs, index, type):
        docs_to_copy = {}

        # print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                docs_to_copy[_id] = existing_doc

        self.index_docs(docs_to_copy)

        self.processed_doc_count += len(docs)


        progress = ((self.processed_doc_count / float(self.total_doc_count)) * 100)
        print '---------------------------------------------------------------------------------------------'
        print 'Progress', self.processed_doc_count, '/', self.total_doc_count, progress, '%'
        print '---------------------------------------------------------------------------------------------'

    def export_doc_ids(self, server, src_index, src_type):
        print 'Fetching doc ids for', src_index, src_type
        query = {
            "match_all": {}
        }
        self.data_utils.batch_fetch_ids_for_query(base_url=server, index=src_index, type=src_type, query=query, ids_fetched=self.ids_fetched)

        # print 'Done, fetched', len(documents_ids), 'doc ids'

    def ids_fetched(self, ids, index, type):
        self.copy_docs_batch(ids)

    def create_destination_index(self, mapping=None):
        if mapping is None:
            # Get mapping from src index
            mapping = self.src_data_loader_utils.get_mapping_from_server()

        if not self.dest_data_loader_utils.index_exists():
            print 'Creating index'
            self.dest_data_loader_utils.put_mapping(mapping)
            # migrate_index(self.dest_data_loader_utils.index)
        else:
            print self.dest_data_loader_utils.index, 'exists'

    def copy_docs(self):
        self.processed_doc_count = 0
        self.total_doc_count = self.get_total_doc_count()

        print 'Total doc count', self.total_doc_count

        self.create_destination_index(mapping=None)

        self.export_doc_ids(server=self.src_data_loader_utils.server,
                            src_index=self.src_data_loader_utils.index,
                            src_type=self.src_data_loader_utils.type)

    def copy_docs_for_ids(self, doc_ids, mapping=None):
        self.processed_doc_count = 0
        self.total_doc_count = len(doc_ids)

        print 'Total doc count', self.total_doc_count

        self.create_destination_index(mapping)

        print 'Fetching docs from source index'
        batch_doc_processor = BatchDocProcessor(doc_ids, self.copy_docs_batch, 3000, 16, 0.33)
        batch_doc_processor.run()

    def copy_docs_batch(self, doc_ids):
        self.data_utils.batch_fetch_docs_for_ids(base_url=self.src_data_loader_utils.server,
                                            ids=doc_ids,
                                            index=self.src_data_loader_utils.index,
                                            type=self.src_data_loader_utils.type,
                                            docs_fetched=self.docs_fetched)

    def index_docs(self, docs_to_copy):
        bulk_data = ''
        count = 0

        for es_id in docs_to_copy:
            count += 1
            doc = docs_to_copy[es_id]
            bulk_data += self.dest_data_loader_utils.bulk_index_header(es_id)
            bulk_data += '\n'
            bulk_data += json.dumps(doc)
            bulk_data += '\n'

            # if count % 1000 == 0:
            #     print 'Processed', 1000, 'docs'

            if len(bulk_data) >= 150000:
                self.load_bulk_data(bulk_data)
                # print 'Copied', count, 'docs'
                bulk_data = ''

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)

        # print 'Copied', count, 'docs'

    def load_bulk_data(self, bulk_data):
        # print 'Bulk data size', len(bulk_data), 'loading...'
        response = self.dest_data_loader_utils.load_bulk_data(bulk_data)

        if response:
            pass
            # print 'Done loading bulk data, saving response'
        else:
            print 'Bulk data load failed'