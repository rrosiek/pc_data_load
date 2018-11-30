import json
from utils import data_utils
from utils import file_utils
from load_config import *

from utils.data_loader_utils import DataLoaderUtils

from constants import DATA_LOADER_BATCH_PREFIX

OP_INDEX = 'index'
OP_UPDATE = 'update'


class DataLoader(object):
    def __init__(self, load_config, data_loader_batch, _index, _type, data_source_batch_name=None):
        self.load_config = load_config

        self.data_loader_batch = data_loader_batch
        self.index = _index
        self.type = _type

        self.data_source_batch_directory = self.load_config.data_source_batch_directory(data_source_batch_name)
        self.failed_docs_directory = self.load_config.failed_docs_directory(data_source_batch_name)
        self.loaded_docs_directory = self.load_config.loaded_docs_directory(data_source_batch_name)
        self.bulk_update_response_directory = self.load_config.bulk_update_response_directory(data_source_batch_name)

        self.existing_docs = {}

        self.failed_docs = {}
        self.updated_ids = {}
        self.indexed_ids = {}

        self.allow_doc_creation = self.load_config.data_mapper.allow_doc_creation(self.load_config.data_source_name)
        self.create_only = self.load_config.data_mapper.create_only(self.load_config.data_source_name)

        self.data_loader_utils = None

    def get_es_id(self, doc_id):
        return self.load_config.data_mapper.get_es_id(doc_id)
    
    def get_doc_id(self, es_id):
        return self.load_config.data_mapper.get_doc_id(es_id)

    def docs_fetched(self, docs, index, type):
        self.load_config.log(LOG_LEVEL_TRACE, 'Docs fetched', len(docs))
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                self.existing_docs[_id] = existing_doc

    def run(self):
        self.data_loader_utils = DataLoaderUtils(self.load_config.server,
                                                 self.index,
                                                 self.type)

        count = 0
        bulk_data = ''

        ids_to_load = self.data_loader_batch.keys()

        if not self.create_only:
            # Create ids to fetch
            ids_to_fetch = []
            for _id in ids_to_load:
                es_id = self.get_es_id(_id)
                ids_to_fetch.append(es_id)

            # Fetch ids
            self.load_config.log(LOG_LEVEL_TRACE, 'Fetching docs', self.load_config.server, self.index, self.type)

            data_utils.batch_fetch_docs_for_ids(self.load_config.server,
                                                ids_to_fetch,
                                                self.index,
                                                self.type,
                                                self.docs_fetched,
                                                self.load_config.doc_fetch_batch_size)

        for _id in ids_to_load:
            data_for_id = self.data_loader_batch[_id]
            es_id = self.get_es_id(_id)

            if es_id in self.existing_docs:
                # Update doc
                existing_doc = self.existing_docs[es_id]
                doc = self.load_config.data_mapper.update_doc(existing_doc=existing_doc,
                                                              _id=_id,
                                                              data_source_name=self.load_config.data_source_name,
                                                              data=data_for_id)
                if self.load_config.test_mode and count % 2500 == 0:
                    # print 'Existing doc', self.load_manager.data_mapper.extract_fields_from_existing_doc(existing_doc)
                    self.load_config.log(LOG_LEVEL_INFO, 'Data', data_for_id)
                    self.load_config.log(LOG_LEVEL_INFO, '--------------------------------------------------------')
                    self.load_config.log(LOG_LEVEL_INFO, 'Updated doc', doc)

                if len(doc) > 0:
                    bulk_data += self.data_loader_utils.bulk_update_header(es_id)
                    bulk_data += '\n'
                    doc = {
                        'doc': doc
                    }
                    bulk_data += json.dumps(doc)
                    bulk_data += '\n'
                else:
                    self.add_to_failed_docs(_id, data_for_id, 'Data mapper: update doc returned empty')
            elif self.allow_doc_creation:
                # Create new doc
                doc = self.load_config.data_mapper.create_doc(_id=_id,
                                                              data_source_name=self.load_config.data_source_name,
                                                              data=data_for_id)
                if self.load_config.test_mode and count % 2500 == 0:
                    self.load_config.log(LOG_LEVEL_INFO, 'Data', data_for_id)
                    self.load_config.log(LOG_LEVEL_INFO, '--------------------------------------------------------')
                    self.load_config.log(LOG_LEVEL_INFO, 'Updated doc', doc)

                if len(doc) > 0:
                    bulk_data += self.data_loader_utils.bulk_index_header(es_id)
                    bulk_data += '\n'
                    bulk_data += json.dumps(doc)
                    bulk_data += '\n'
                else:
                    self.add_to_failed_docs(_id, data_for_id, 'Data mapper: create doc returned empty')
            else:
                self.add_to_failed_docs(_id, data_for_id, 'Update failed: existing doc not found')

            count += 1
            if count % 500 == 0:
                self.load_config.log(LOG_LEVEL_DEBUG, 'Processed', count, 'docs')

            if len(bulk_data) >= self.load_config.bulk_data_size:
                self.load_bulk_data(bulk_data)
                bulk_data = ''

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)

        if not self.load_config.test_mode:
            self.save_summary(ids_to_load)

    def load_bulk_data(self, bulk_data):
        self.load_config.log(LOG_LEVEL_DEBUG, 'Bulk data size', len(bulk_data), 'loading...')
        response = None
        if not self.load_config.test_mode:
            response = self.data_loader_utils.load_bulk_data(bulk_data)

        if response:
            self.load_config.log(LOG_LEVEL_DEBUG, 'Done loading bulk data, saving response')
            if not self.load_config.test_mode:
                # Extract and save the failed docs
                self.process_bulk_update_response(response)
        else:
            self.load_config.log(LOG_LEVEL_ERROR, 'Bulk data load failed')

    def process_response_item(self, item, op):
        item_op = item[op]
        es_id = item_op['_id']
        _id = self.get_doc_id(es_id)
        try:
            doc = self.data_loader_batch[es_id]
        except Exception as e:
            doc = self.data_loader_batch[_id]

        if 'status' in item_op:
            if item_op['status'] == 200 or item_op['status'] == 201:
                # doc success
                if op == OP_INDEX:
                    self.indexed_ids[_id] = 0
                elif op == OP_UPDATE:
                    self.updated_ids[_id] = 0
            else:
                self.add_to_failed_docs(_id, doc, item)
        else:
            self.add_to_failed_docs(_id, doc, item)

    def process_bulk_update_response(self, response):
        load_summary = json.loads(response)
        items = load_summary['items']
        # print load_summary
        for item in items:
            if OP_INDEX in item:
                self.process_response_item(item, OP_INDEX)
            elif OP_UPDATE in item:
                self.process_response_item(item, OP_UPDATE)

        # save response to file
        self.load_config.log(LOG_LEVEL_TRACE,
                        'Updated ids:', len(self.updated_ids),
                        'Indexed ids:', len(self.indexed_ids),
                        'Failed ids:', len(self.failed_docs))
        bulk_update_response_file_name = file_utils.batch_file_name_with_prefix('summary')
        file_utils.save_text_file(self.bulk_update_response_directory,
                                  bulk_update_response_file_name + '.json',
                                  response)

    def save_summary(self, ids_to_load):
        data_loader_batch_name = file_utils.batch_file_name_with_prefix(DATA_LOADER_BATCH_PREFIX)

        # Find skipped ids
        for _id in ids_to_load:
            if _id not in self.updated_ids and _id not in self.indexed_ids and _id not in self.failed_docs:
                doc = self.data_loader_batch[_id]
                self.add_to_failed_docs(_id, doc, 'Skipped')

        # Save failed docs
        if len(self.failed_docs) > 0:
            file_utils.save_file(self.failed_docs_directory, data_loader_batch_name + '.json', self.failed_docs)
      
        # Save batch summary
        summary = {
            'indexed_ids': self.indexed_ids.keys(),
            'updated_ids': self.updated_ids.keys(),
        }

        file_utils.save_file(self.loaded_docs_directory, data_loader_batch_name + '.json', summary)

        # Print summary
        self.load_config.log(LOG_LEVEL_INFO,
                        '---------------------------------------------------------------------------------------------')
        self.load_config.log(LOG_LEVEL_INFO,
                        self.load_config.server,
                        self.load_config.index,
                        self.load_config.type,
                        ' Updated docs:',
                        len(self.updated_ids) + len(self.indexed_ids),
                        ', Failed docs:', len(self.failed_docs))
        self.load_config.log(LOG_LEVEL_INFO,
                        '---------------------------------------------------------------------------------------------')

    def add_to_failed_docs(self, _id, doc, reason):
        data_for_id = {
            'reason': reason,
            'doc': doc
        }

        self.failed_docs[_id] = data_for_id


def start_data_load(load_config, data_loader_batch, _index, _type, data_source_batch_name):
    data_loader = DataLoader(load_config=load_config,
                             data_loader_batch=data_loader_batch,
                             _index=_index,
                             _type=_type,
                             data_source_batch_name=data_source_batch_name)
    data_loader.run()