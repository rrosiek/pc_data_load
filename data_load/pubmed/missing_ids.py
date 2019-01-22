
from data_load.base.constants import DATA_LOADING_DIRECTORY, ID_PUBMED
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig
from data_load.base.utils.export_doc_ids import export_doc_ids
from data_load.base.utils.data_utils import DataUtils
from data_load.base.utils.data_loader_utils import DataLoaderUtils

import psutil
import sys
import json

from data_load.base.utils import file_utils


ID_PUBMED_2019 = 'PUBMED_2019'

SERVER = 'http://localhost:9200'
OLD_INDEX = 'pubmed2018_v5'
OLD_TYPE = 'article'

NEW_INDEX = 'pubmed2019'
NEW_TYPE = 'article'


missing_ids_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'missing_ids'


class FindMissingIds(object):

    def __init__(self):
        self.missing_ids = {}
        self.new_ids = {}
        self.data_utils = DataUtils()
        self.data_loader_utils = DataLoaderUtils(SERVER, OLD_INDEX, OLD_TYPE, '', '')

        self.docs_for_dolan = {}

    def run(self):
        old_ids = export_doc_ids(server=SERVER,
                                src_index=OLD_INDEX,
                                src_type=OLD_TYPE)

        new_ids = export_doc_ids(server=SERVER,
                                src_index=NEW_INDEX,
                                src_type=NEW_TYPE)

        for _id in old_ids:
            if _id not in new_ids:
                self.missing_ids[_id] = 0
                if len(self.missing_ids) % 1000 == 0:
                    print 'Missing ids', len(self.missing_ids)

        for _id in new_ids:
            if _id not in old_ids:
                self.new_ids[_id] = 0
                if len(self.new_ids) % 1000 == 0:
                    print 'New ids', len(self.new_ids)

        print 'Missing ids', len(self.missing_ids)
        print 'New ids', len(self.new_ids)

        file_utils.make_directory(missing_ids_directory)

        file_utils.save_file(missing_ids_directory, 'missing_ids.json', self.missing_ids.keys())
        file_utils.save_file(missing_ids_directory, 'new_ids.json', self.new_ids)


    def check_tags_and_annotations(self):
        missing_ids = file_utils.load_file(missing_ids_directory, 'missing_ids.json')
        new_ids = file_utils.load_file(missing_ids_directory, 'new_ids.json')
        
        print 'Missing ids', len(missing_ids)
        print 'New ids', len(new_ids)

        docs_with_tags = self.fetch_ids()

        missing_docs_with_tags = []
        for _id in missing_ids:
            if _id in docs_with_tags:
                missing_docs_with_tags.append(_id)
                print 'Missing docs with tags', _id

        print 'Missing docs with tags', len(missing_docs_with_tags)
        print 'Missing docs with tags', json.dumps(missing_docs_with_tags)

        for _id in missing_docs_with_tags:
            existing_doc = self.get_existing_doc(_id)
            if 'userTags' in existing_doc:
                user_tags = existing_doc['userTags']
                for user_tag in user_tags:
                    added_by = user_tag['added_by']

                    if added_by == 'ghoshd1@niaid.nih.gov':
                        self.docs_for_dolan[_id] = existing_doc
                        print _id
                        print user_tags

                    break

        print 'Docs for Dolan', len(self.docs_for_dolan)

        print 'Docs for Dolan', self.docs_for_dolan.keys()

    def get_existing_doc(self, _id):
        exisiting_doc = self.data_loader_utils.fetch_doc(_id)
        if exisiting_doc is not None and '_source' in exisiting_doc:
            exisiting_doc = exisiting_doc['_source']
        return exisiting_doc


    def fetch_ids(self):
        combined_docs = {}

        tags_query = self.tags_query()
        annotations_query = self.annotations_query()

        print 'Fetching docs with tags', SERVER, OLD_INDEX, OLD_TYPE
        docs_with_tags = self.data_utils.batch_fetch_ids_for_query(base_url=SERVER,
                                                                    query=tags_query,
                                                                    index=OLD_INDEX,
                                                                    type=OLD_TYPE,
                                                                    ids_fetched=self.ids_fetched,
                                                                    batch_size=1000)
        print len(docs_with_tags), 'docs_with_tags'
        for _id in docs_with_tags:
            combined_docs[_id] = ''

        print 'Fetching docs with annotations', SERVER, OLD_INDEX, OLD_TYPE
        docs_with_annotations = self.data_utils.batch_fetch_ids_for_query(base_url=SERVER,
                                                                            query=annotations_query,
                                                                            index=OLD_INDEX,
                                                                            type=OLD_TYPE,
                                                                            ids_fetched=self.ids_fetched,
                                                                            batch_size=1000)

        print len(docs_with_annotations), 'docs_with_annotations'
        for _id in docs_with_annotations:
            combined_docs[_id] = ''

        print len(combined_docs), 'combined_docs'
        return combined_docs

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

find_missing_ids = FindMissingIds()
find_missing_ids.check_tags_and_annotations()


