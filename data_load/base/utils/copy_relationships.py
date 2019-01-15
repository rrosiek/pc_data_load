from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.utils.data_utils  import DataUtils
from data_load.base.utils.batch_doc_processor import BatchDocProcessor

import requests
import json
import time
import data_load.base.utils.file_utils
from data_load.migrate_indices import migrate_index


from data_load.base.constants import RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS, RELATIONSHIP_TYPE_RELATIONS, ID_PUBMED

class CopyRelationships(object):

    def __init__(self, src_server, dest_server, src_index, src_type, dst_index, dst_type, username, password):
        self.src_data_loader_utils = DataLoaderUtils(src_server, src_index, src_type)
        self.dest_data_loader_utils = DataLoaderUtils(dest_server, dst_index, dst_type)

        self.processed_doc_count = 0
        self.total_doc_count = 0

        self.data_utils = DataUtils()

        self.relations_to_exclude = []
        self.missing_destination_ids = []

        self.username = username
        self.password = password

    def run(self):
        self.processed_doc_count = 0
        self.total_doc_count = self.get_total_doc_count()

        print 'Total doc count', self.total_doc_count

        # self.create_destination_index(mapping=None)
        self.export_doc_ids(server=self.src_data_loader_utils.server,
                            src_index=self.src_data_loader_utils.index,
                            src_type=self.src_data_loader_utils.type)


    def run_for_ids(self, doc_ids, mapping=None):
        self.processed_doc_count = 0
        self.total_doc_count = len(doc_ids)

        print 'Total doc count', self.total_doc_count

        print 'Fetching docs from source index'
        batch_doc_processor = BatchDocProcessor(doc_ids, self.copy_docs_batch, 1000, 2, 0)
        batch_doc_processor.run()

    def export_doc_ids(self, server, src_index, src_type):
        print 'Fetching doc ids for', src_index, src_type
        query = {
            "match_all": {}
        }
        self.data_utils.batch_fetch_ids_for_query(base_url=server, index=src_index, type=src_type, query=query, ids_fetched=self.ids_fetched, batch_size=10000)

        # print 'Done, fetched', len(documents_ids), 'doc ids'

    def ids_fetched(self, ids, index, type):
        print 'Ids fetched', len(ids)
        # self.copy_docs_batch(ids)

        print 'Fetching docs from source index'
        batch_doc_processor = BatchDocProcessor(ids, self.copy_docs_batch, 1000, 4, 0.5)
        batch_doc_processor.run()

    def copy_docs_batch(self, doc_ids):
        print 'Fetching docs'
        self.data_utils.batch_fetch_docs_for_ids(base_url=self.src_data_loader_utils.server,
                                                ids=doc_ids,
                                                index=self.src_data_loader_utils.index,
                                                type=self.src_data_loader_utils.type,
                                                docs_fetched=self.docs_fetched)

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

        # Update progress
        self.processed_doc_count += len(docs)
        progress = ((self.processed_doc_count / float(self.total_doc_count)) * 100)
        print '---------------------------------------------------------------------------------------------'
        print 'Progress', self.processed_doc_count, '/', self.total_doc_count, progress, '%'
        print '---------------------------------------------------------------------------------------------'

    def get_src_relations(self, src_doc, relationship_type):
        src_relations = []

        if relationship_type in src_doc:
            relations = src_doc[relationship_type]

            for relation_item in relations:
                exclude_relation_item = False
                for relation_to_exclude in self.relations_to_exclude:
                    if relation_to_exclude['source'] == relation_item['source'] and relation_to_exclude['index_id'] == relation_item['index_id']:
                        exclude_relation_item = True
                        break

                if not exclude_relation_item:
                    src_relations.append(relation_item)

        return src_relations

    def get_dest_relations(self, dest_doc, relationship_type):
        dest_relations = []

        if relationship_type in dest_doc:
            dest_relations = dest_doc[relationship_type]

        return dest_relations

    def add_relations(self, append_ids, relation, relations_list):
        relation_found = False
        for existing_relation in relations_list:
            # print existing_relation['source'], relation['source'], existing_relation['index_id'], relation['index_id']
            if existing_relation['source'] == relation['source'] and existing_relation['index_id'] == relation['index_id']:
                existing_relation_ids = existing_relation['ids']

                if append_ids:
                    relation_ids = relation['ids']

                    for _id in relation_ids:
                        if _id not in existing_relation_ids:
                            existing_relation_ids.append(_id)

                existing_relation['ids'] = existing_relation_ids   

                relation_found = True
                break

        if not relation_found:
            relations_list.append(relation)

        return relations_list

    def merge_relations(self, src_doc, dest_doc, relationship_type):
        dest_relations = self.get_dest_relations(dest_doc, relationship_type)
        src_relations = self.get_src_relations(src_doc, relationship_type)

        # print 'src_relations', len(src_relations)
        # print 'dest_relations', len(dest_relations)

        combined_relations = []
        for relation in dest_relations:
            combined_relations = self.add_relations(True, relation, combined_relations)

        for relation in src_relations:
            combined_relations = self.add_relations(True, relation, combined_relations)


        return combined_relations

    def copy_relations(self, src_docs):
        bulk_data = ''
        count = 0

        # Fetch destination docs
        destination_ids = src_docs.keys()
        destination_docs_array = self.data_utils.fetch_docs_for_ids(base_url=self.dest_data_loader_utils.server, 
                                                                    ids=destination_ids, 
                                                                    index=self.dest_data_loader_utils.index, 
                                                                    type=self.dest_data_loader_utils.type, 
                                                                    username=self.username, 
                                                                    password=self.password)
        
        # Create destination doc dict
        destination_docs = {}
        for doc in destination_docs_array:
            _id = doc['_id']
            if '_source' in doc:
                destination_docs[_id] = doc['_source']

        # Find missing destination docs
        for _id in destination_ids:
            if _id not in destination_docs:
                self.missing_destination_ids.append(_id)

        print 'Missing ids', len(self.missing_destination_ids)
        # print 'dest ids', len()

        # Copy relations 
        for _id in destination_docs:
            dest_doc = destination_docs[_id]
            src_doc = src_docs[_id]
            
            dest_relations = {}

            dest_relations[RELATIONSHIP_TYPE_CITATIONS] = self.merge_relations(src_doc, dest_doc, RELATIONSHIP_TYPE_CITATIONS)
            dest_relations[RELATIONSHIP_TYPE_CITED_BYS] = self.merge_relations(src_doc, dest_doc, RELATIONSHIP_TYPE_CITED_BYS)
            dest_relations[RELATIONSHIP_TYPE_RELATIONS] = self.merge_relations(src_doc, dest_doc, RELATIONSHIP_TYPE_RELATIONS)

            doc = {}
            if len(dest_relations[RELATIONSHIP_TYPE_CITATIONS]) > 0:
                doc[RELATIONSHIP_TYPE_CITATIONS] = dest_relations[RELATIONSHIP_TYPE_CITATIONS]

            if len(dest_relations[RELATIONSHIP_TYPE_CITED_BYS]) > 0:
                doc[RELATIONSHIP_TYPE_CITED_BYS] = dest_relations[RELATIONSHIP_TYPE_CITED_BYS]

            if len(dest_relations[RELATIONSHIP_TYPE_RELATIONS]) > 0:
                doc[RELATIONSHIP_TYPE_RELATIONS] = dest_relations[RELATIONSHIP_TYPE_RELATIONS]

            # if len(dest_relations[RELATIONSHIP_TYPE_CITATIONS]) >= 2:
            #     print _id

            count += 1

            # doc = docs_to_copy[es_id]
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
                # self.load_bulk_data(bulk_data)
                # print 'Copied', count, 'docs'
                bulk_data = ''

        if len(bulk_data) > 0:
            # self.load_bulk_data(bulk_data)
            pass

        # print 'Copied', count, 'docs'


    # def create_destination_index(self, mapping=None):
    #     if mapping is None:
    #         # Get mapping from src index
    #         mapping = self.src_data_loader_utils.get_mapping_from_server()

    #     if not self.dest_data_loader_utils.index_exists():
    #         print 'Creating index'
    #         self.dest_data_loader_utils.put_mapping(mapping)
    #         # migrate_index(self.dest_data_loader_utils.index)
    #     else:
    #         print self.dest_data_loader_utils.index, 'exists'

   

    def load_bulk_data(self, bulk_data):
        # print 'Bulk data size', len(bulk_data), 'loading...'
        response = self.dest_data_loader_utils.load_bulk_data(bulk_data)

        if response:
            pass
            # print 'Done loading bulk data, saving response'
        else:
            print 'Bulk data load failed'

    def get_total_doc_count(self):
        return self.data_utils.get_total_doc_count(base_url=self.src_data_loader_utils.server,
                                            index=self.src_data_loader_utils.index,
                                            type=self.src_data_loader_utils.type)



src_server = 'http://localhost:9200'
src_index = 'pubmed2018_v5'
src_type = 'article'

dest_server = 'http://localhost:9200'
dest_index = 'pubmed2019'
dest_type = 'article'

copy_relations = CopyRelationships(src_server=src_server, 
                                    dest_server=dest_server, 
                                    src_index=src_index, 
                                    src_type=src_type, 
                                    dst_index=dest_index, 
                                    dst_type=dest_type, 
                                    username='', 
                                    password='')


copy_relations.relations_to_exclude.append({
    "source": "",
    "index_id": ID_PUBMED
})
copy_relations.run()
# copy_relations.run_for_ids([28863354])