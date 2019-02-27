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

class CopyTagsAndAnnotations(object):
    def __init__(self, reports_directory, src_server, src_index, src_type, dest_server, dest_index, dest_type):
        self.data_loader_utils_dest = DataLoaderUtils(dest_server, dest_index, dest_type)
        self.reports_directory = reports_directory

        self.src_server = src_server
        self.src_index = src_index
        self.src_type = src_type

        self.dest_server = dest_server
        self.dest_index = dest_index
        self.dest_type = dest_type

        self.copy_tags = True
        self.copy_annotations = True

        self.combine_tags = False # Combine not implemented, set to false  
        self.combine_annotations = False # Combine not implemented, set to false 

        self.data_utils = DataUtils()

    def run(self):
        docs_to_copy = self.fetch_ids()
        self.copy(docs_to_copy)

        if self.copy_tags:
            self.verify_tags()

        if self.copy_annotations:
            self.verify_annotations()

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

        if self.copy_tags:
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

        if self.copy_annotations:
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
            if self.copy_tags and 'userTags' in existing_doc:
                if self.combine_tags:
                    pass                    
                else:
                    doc['userTags'] = existing_doc['userTags']

            if self.copy_annotations and 'annotations' in existing_doc:
                if self.combine_annotations:
                    pass    
                else:
                    doc['annotations'] = existing_doc['annotations']

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

    def copy(self, ids):
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

    def verify_tags(self):
        tags_query = self.tags_query()

        src_docs_with_tags = self.data_utils.batch_fetch_ids_for_query(base_url=self.src_server,
                                                                  query=tags_query,
                                                                  index=self.src_index,
                                                                  type=self.src_type,
                                                                  ids_fetched=self.ids_fetched,
                                                                  batch_size=1000)

        dest_docs_with_tags = self.data_utils.batch_fetch_ids_for_query(base_url=self.dest_server,
                                                                   query=tags_query,
                                                                   index=self.dest_index,
                                                                   type=self.dest_type,
                                                                   ids_fetched=self.ids_fetched,
                                                                   batch_size=1000)

        print len(src_docs_with_tags), 'src_docs_with_tags'
        print len(dest_docs_with_tags), 'dest_docs_with_tags'

        dest_dict = {}
        for _id in dest_docs_with_tags:
            dest_dict[_id] = 0

        missing_ids = []
        for _id in src_docs_with_tags:
            if _id not in dest_dict:
                missing_ids.append(_id)

        # print missing_ids

        print len(missing_ids), 'tags missing_ids'
        count = 0
        for _id in missing_ids:
            count += 1
            if count % 10000:
                print _id

        file_utils.save_file(self.reports_directory, 'tags_missing_ids.json', missing_ids)

    def verify_annotations(self):
        annotations_query = self.annotations_query()

        src_docs_with_tags = self.data_utils.batch_fetch_ids_for_query(base_url=self.src_server,
                                                                        query=annotations_query,
                                                                        index=self.src_index,
                                                                        type=self.src_type,
                                                                        ids_fetched=self.ids_fetched,
                                                                        batch_size=1000)

        dest_docs_with_tags = self.data_utils.batch_fetch_ids_for_query(base_url=self.dest_server,
                                                                        query=annotations_query,
                                                                        index=self.dest_index,
                                                                        type=self.dest_type,
                                                                        ids_fetched=self.ids_fetched,
                                                                        batch_size=1000)

        print len(src_docs_with_tags), 'src_docs_with_annotations'
        print len(dest_docs_with_tags), 'dest_docs_with_annotations'

        dest_dict = {}
        for _id in dest_docs_with_tags:
            dest_dict[_id] = 0

        missing_ids = []
        for _id in src_docs_with_tags:
            if _id not in dest_dict:
                missing_ids.append(_id)

        print len(missing_ids), 'annotations missing_ids'

        count = 0
        for _id in missing_ids:
            count += 1
            if count % 10:
                print _id

        file_utils.save_file(self.reports_directory, 'annotations_missing_ids.json', missing_ids)


