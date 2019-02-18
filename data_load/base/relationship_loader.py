import json
import os

from constants import RELATIONSHIP_TYPE_RELATIONS
from constants import RELATIONSHIP_TYPE_CITATIONS
from constants import RELATIONSHIP_TYPE_CITED_BYS

from utils.data_utils import DataUtils
from utils import file_utils
from utils.data_loader_utils import DataLoaderUtils

from data_loader import DataLoader
from load_config import *


class RelationshipLoader(DataLoader):
    def __init__(self, load_config, data_loader_batch, _index, _type, data_source_batch_name=None):
        super(RelationshipLoader, self).__init__(load_config, data_loader_batch, _index, _type, data_source_batch_name)

        self.append = load_config.append_relations
        self.source = load_config.source

        self.index = _index
        self.type = _type

        # self.data_source_batch_directory = self.data_source_batch_directory + '/' + self.source_index_id
        # file_utils.make_directory(self.data_source_batch_directory)

        self.data_loader_utils = None
        self.data_utils = None

    def get_es_id(self, doc_id):
        return doc_id

    def get_doc_id(self, es_id):
        return es_id

    def run(self):
        self.data_loader_utils = DataLoaderUtils(self.load_config.server,
                                                 self.index,
                                                 self.type,
                                                 self.load_config.server_username,
                                                 self.load_config.server_password)

        self.data_utils = DataUtils()

        count = 0
        bulk_data = ''

        ids_to_fetch = self.data_loader_batch.keys()
        self.load_config.log(LOG_LEVEL_TRACE, 'Fetching docs', self.load_config.server, self.index, self.type)

        self.data_utils.batch_fetch_docs_for_ids(self.load_config.server,
                                            ids_to_fetch,
                                            self.index,
                                            self.type,
                                            self.docs_fetched,
                                            self.load_config.doc_fetch_batch_size,
                                            self.load_config.server_username,
                                            self.load_config.server_password)

        for _id in self.existing_docs:
            relations = self.data_loader_batch[_id]
            existing_doc = self.existing_docs[_id]

            doc = {}

            # Update relations
            for relation in relations:
                dest_index_id = relation['index_id']
                dest_ids = relation['ids']
                relationship_type = relation['type']
                ids_to_remove = []
                if 'ids_to_remove' in relation:
                    ids_to_remove = relation['ids_to_remove']

                self.load_config.log(LOG_LEVEL_TRACE, self.index, relationship_type,  dest_index_id, len(dest_ids))
                
                existing_doc = self.load_config.data_mapper.update_relations_for_doc(_id,
                                                                                    existing_doc,
                                                                                    dest_ids,
                                                                                    self.source,
                                                                                    dest_index_id,
                                                                                    relation_type=relationship_type,
                                                                                    append=self.append,
                                                                                    ids_to_remove=ids_to_remove)
                doc[relationship_type] = existing_doc[relationship_type]

            if self.load_config.test_mode and count % 2500 == 0:
                # print 'Existing doc id', _id
                self.load_config.log(LOG_LEVEL_INFO, 'Data', relations)
                self.load_config.log(LOG_LEVEL_INFO, 'Updated doc', doc)

            if len(doc) > 0:
                bulk_update_header = self.data_loader_utils.bulk_update_header(_id)
                self.load_config.log(LOG_LEVEL_TRACE, 'bulk update header:', bulk_update_header)
                self.load_config.log(LOG_LEVEL_TRACE, 'bulk data', doc)

                bulk_data += bulk_update_header
                bulk_data += '\n'
                doc = {
                    'doc': doc
                }
                bulk_data += json.dumps(doc)
                bulk_data += '\n'

            count += 1
            if count % 50 == 0:
                self.load_config.log(LOG_LEVEL_DEBUG, 'Processed docs', count, os.getpid(), self.index, _id)

            if len(bulk_data) >= self.load_config.bulk_data_size:
                self.load_bulk_data(bulk_data)
                bulk_data = ''

        if len(bulk_data) > 0:
            self.load_bulk_data(bulk_data)

        # logger.log(1,  'Process completed, saving loaded ids.........................')

        if not self.load_config.test_mode:
            self.save_summary(ids_to_fetch)


def start_relationship_load(load_config, data_loader_batch, _index, _type, data_source_batch_name):
    relationship_loader = RelationshipLoader(load_config=load_config,
                                             data_loader_batch=data_loader_batch,
                                             _index=_index,
                                             _type=_type,
                                             data_source_batch_name=data_source_batch_name)
    relationship_loader.run()
