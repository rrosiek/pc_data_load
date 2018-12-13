from data_utils import DataUtils
import file_utils
import requests
import time
import json
import os
from random import randint

from data_loader_utils import DataLoaderUtils

# SRC_INDEX = 'pubmed2017_v4'
# SRC_TYPE = 'article'
#
# DEST_INDEX = 'pubmed2018'
# DEST_TYPE = 'article'

# LOCAL_SERVER = 'http://localhost:9200'

# SERVER = 'http://52.202.35.240:9200'
# SEC_SERVER = 'http://54.89.157.2:9200'

# data_loader_utils_src = DataLoaderUtils(LOCAL_SERVER, DEST_INDEX, DEST_TYPE)

# reports_directory = 'reports'


class DeleteUserData(object):
    def __init__(self, reports_directory, src_server, src_index, src_type):
        self.data_loader_utils_dest = DataLoaderUtils(src_server, src_index, src_type)
        self.reports_directory = reports_directory

        self.src_server = src_server
        self.src_index = src_index
        self.src_type = src_type

        self.delete_tags = True
        self.delete_annotations = True

        self.data_utils = DataUtils()

    def run(self):
        docs_to_delete = self.fetch_ids()
        self.delete(docs_to_delete)

    def ids_fetched(self, ids, index, type):
        print len(ids), 'ids fetched'

    def tags_query(self):
        tags_query = {
            "nested": {
                "path": "userTags",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "exists": {
                                    "field": "userTags"
                                }
                            }
                        ]
                    }
                }
            }
        }

        return tags_query

    def annotations_query(self):
        annotations_query = {
            "nested": {
                "path": "annotations",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "exists": {
                                    "field": "annotations"
                                }
                            }
                        ]
                    }
                }
            }
        }

        return annotations_query

    def fetch_ids(self):
        combined_docs = {}

        tags_query = self.tags_query()
        annotations_query = self.annotations_query()

        if self.delete_tags:
            print 'Fetching docs with tags', self.src_server, self.src_index, self.src_type
            docs_with_tags = self.data_utils.batch_fetch_ids_for_query(base_url=self.src_server,
                                                                  query=tags_query,
                                                                  index=self.src_index,
                                                                  type=self.src_type,
                                                                  ids_fetched=self.ids_fetched,
                                                                  batch_size=1000)
            print len(docs_with_tags), 'docs_with_tags'
            for _id in docs_with_tags:
                combined_docs[_id] = ''

        if self.delete_annotations:
            print 'Fetching docs with annotations', self.src_server, self.src_index, self.src_type
            docs_with_annotations = self.data_utils.batch_fetch_ids_for_query(base_url=self.src_server,
                                                                         query=annotations_query,
                                                                         index=self.src_index,
                                                                         type=self.src_type,
                                                                         ids_fetched=self.ids_fetched,
                                                                         batch_size=1000)

            print len(docs_with_annotations), 'docs_with_annotations'
            for _id in docs_with_annotations:
                combined_docs[_id] = ''

        print len(combined_docs), 'combined_docs'
        return combined_docs

    def delete_data(self, _ids):
        bulk_data = ''
        for _id in _ids:
            doc = {}
            if self.delete_tags:
                doc['userTags'] = []

            if self.delete_annotations:
                doc['annotations'] = []

            if len(doc) > 0:
                bulk_data += self.data_loader_utils_dest.bulk_update_header(_id)
                bulk_data += '\n'
                doc = {
                    'doc': doc
                }
                bulk_data += json.dumps(doc)
                bulk_data += '\n'

            if len(bulk_data) >= 300000:
                self.load_bulk_data(bulk_data)
                bulk_data = ''

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)

    def docs_fetched(self, docs, index, type):
        print len(docs), 'docs fetched'
        existing_docs = {}
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                existing_docs[_id] = existing_doc

        bulk_data = ''
        for _id in existing_docs:
            existing_doc = existing_docs[_id]

            doc = {}
            if self.delete_tags and 'userTags' in existing_doc:
                doc['userTags'] = []

            if self.delete_annotations and 'annotations' in existing_doc:
                doc['annotations'] = []

            if len(doc) > 0:
                bulk_data += self.data_loader_utils_dest.bulk_update_header(_id)
                bulk_data += '\n'
                doc = {
                    'doc': doc
                }
                bulk_data += json.dumps(doc)
                bulk_data += '\n'

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)

        batch_file_name = file_utils.batch_file_name_with_prefix('loaded_ids_') + '.json'
        file_utils.save_file(self.reports_directory, batch_file_name, existing_docs.keys())

    def delete(self, ids):
        file_utils.make_directory(self.reports_directory)
        ids_array = ids.keys()
        # ids_array = [ids_array[0]]
        self.data_utils.batch_fetch_docs_for_ids(base_url=self.src_server,
                                            ids=ids_array,
                                            index=self.src_index,
                                            type=self.src_type,
                                            docs_fetched=self.docs_fetched, batch_size=500)

    def load_bulk_data(self, bulk_data):
        print 'Bulk data size', len(bulk_data), 'loading...'
        response = self.data_loader_utils_dest.load_bulk_data(bulk_data)

        if response:
            print 'Done loading bulk data, saving response'
        else:
            print 'Bulk data load failed'


