import data_load.base.utils.file_utils as file_utils
from data_load.base.utils.data_utils import DataUtils

from data_load.base.utils import export_doc_ids
from data_load.base.constants import *
from data_load.irdb.config import *
import data_load.irdb.irdb_load_config as irdb_load_config

import json
import requests
import os

from multiprocessing import Pool

session = requests.Session()

RELATION_INDEXES = [
    # ID_USPTO,
    ID_CLINICAL_TRIALS,
    ID_CLINICAL_GUIDELINES,
    ID_FDA_PURPLE_BOOK,
    ID_FDA_PATENTS,
    ID_FDA_PRODUCTS,
    # ID_DERWENT_PATENTS,
    ID_DWPI
]

IRDB_IDS_FILE_NAME = 'DOC_IDS_' + ID_IRDB + '.json'

BATCH_DOC_COUNT = 1000

def get_load_config():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DS_EXTENDED_RELATIONS
    return load_config

def filter_and_split_ids_into_batches(load_config):
    other_files_directory = load_config.other_files_directory()
    generated_files_directory = load_config.data_source_directory()

    all_ids = export_doc_ids.get_doc_ids_for_load_config(load_config)
    # total_count = len(all_ids)

    max_batch_count = BATCH_DOC_COUNT

    batch_index = 0
    batch_ids = []
    # Splitting into batches
    for _id in all_ids:
        batch_ids.append(_id)

        if len(batch_ids) >= max_batch_count:
            print 'Writing batch:', batch_index
            batch_file_name = 'batch_' + str(batch_index) + '.json'
            file_utils.save_file(generated_files_directory, batch_file_name, batch_ids)

            batch_ids = []
            batch_index += 1

    if len(batch_ids) > 0:
        print 'Writing batch:', batch_index
        batch_file_name = 'batch_' + str(batch_index) + '.json'
        file_utils.save_file(generated_files_directory, batch_file_name, batch_ids)

        batch_index += 1

    print batch_index, 'batches to process'


class RelationsProcessor:

    def __init__(self, load_config, batch_file_name, session):
        self.load_config = load_config
        self.batch_file_name = batch_file_name
        self.session = session
        self.irdb_docs = {}
        self.pubmed_docs = {}

        self.missing_pubmed_ids = []

    def fetch_doc(self, _id, index_id, field=None):
        # print('Fetching doc: ' + _id)
        url = LOCAL_SERVER + '/' + INDEX_MAPPING[index_id]['index'] + '/' + INDEX_MAPPING[index_id]['type'] + '/' + str(_id)
        if field is not None:
            url += '?_source=' + field

        # print(url)
        response = self.session.get(url)
        # print(str(response.status_code))
        # print(str(response.text))
        if response.status_code == 200 or response.status_code == 201:
            return json.loads(response.text)
        else:
            print(str(response.text))

        return None

    def docs_fetched_irdb(self, docs, index, type):
        print 'IRDB Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                irdb_doc = doc['_source']
                self.irdb_docs[_id] = irdb_doc

    def docs_fetched_pubmed(self, docs, index, type):
        # print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                pubmed_doc = doc['_source']
                self.pubmed_docs[_id] = pubmed_doc
   
    def batch_fetch_docs(self, ids, index_id):
        data_utils = DataUtils()
        if index_id == ID_IRDB:
            data_utils.batch_fetch_docs_for_ids(LOCAL_SERVER,
                                                ids,
                                                INDEX,
                                                TYPE,
                                                self.docs_fetched_irdb, 1000)
        elif index_id == ID_PUBMED:
            data_utils.batch_fetch_docs_for_ids(SERVER,
                                                ids,
                                                INDEX_MAPPING[index_id]['index'],
                                                INDEX_MAPPING[index_id]['type'],
                                                self.docs_fetched_pubmed, 1000)

    def batch_fetch_pubmed_docs(self, pmids):
        self.pubmed_docs = {}
        self.batch_fetch_docs(pmids, ID_PUBMED)

    def process_irdb_relations(self):
        
        generated_files_directory = self.load_config.data_source_directory()
        # print 'generated_files_directory', generated_files_directory
        all_ids = file_utils.load_file(generated_files_directory, self.batch_file_name)

        print generated_files_directory, self.batch_file_name, len(all_ids)
        
        processed_count = 0
        batch_count = 0

        ids_to_update = file_utils.load_file(generated_files_directory, 'ids_to_update_' + self.batch_file_name)
        processed_ids = file_utils.load_file(generated_files_directory, 'processed_ids_' + self.batch_file_name)

        filtered_ids = []
        for _id in all_ids:
            if _id not in processed_ids:
                filtered_ids.append(_id)

        print 'Processing', self.batch_file_name, len(filtered_ids), 'ids'

        self.batch_fetch_docs(filtered_ids, ID_IRDB)

        for _id in filtered_ids:
            processed_ids[_id] = ''

            processed_count += 1
            batch_count += 1

            if processed_count % 100 == 0:
                print 'Processing irdb', _id

            # print 'Processing', processed_count, '/', total_count
            ex_rl = self.process_id(_id)
            if len(ex_rl) > 0:
                # print ex_rl
                ids_to_update[_id] = ex_rl

            if batch_count >= 100:
                batch_count = 0

                file_utils.save_file(generated_files_directory, 'ids_to_update_' + self.batch_file_name, ids_to_update)
                file_utils.save_file(generated_files_directory, 'processed_ids_' + self.batch_file_name, processed_ids)

        file_utils.save_file(generated_files_directory, 'ids_to_update_' + self.batch_file_name, ids_to_update)
        file_utils.save_file(generated_files_directory, 'processed_ids_' + self.batch_file_name, processed_ids)

        file_utils.save_file(generated_files_directory, 'missing_pubmed_ids_' + self.batch_file_name, self.missing_pubmed_ids)

        print 'Docs to update..............................................', len(ids_to_update)

    def process_id(self, _id):
        extended_relations = {}
        if _id in self.irdb_docs:
            doc = self.irdb_docs[_id]
            if doc is not None:
                if 'cited_bys' in doc:
                    cited_bys = doc['cited_bys']
                    for cited_by in cited_bys:
                        index_id = cited_by['index_id']
                        if index_id == ID_PUBMED:
                            pmids = cited_by['ids']
                            self.batch_fetch_pubmed_docs(pmids)
                            for pmid in pmids:
                                extended_relations = self.process_pmid(pmid, extended_relations)

        return extended_relations

    def process_pmid(self, pmid, extended_relations):
        # print 'Processing pmid', pmid
        if pmid in self.pubmed_docs:
            doc = self.pubmed_docs[pmid]
            if doc is not None:
                if 'cited_bys' in doc:
                    relations = doc['cited_bys']
                    extended_relations = self.process_relations(relations, extended_relations)

                if 'citations' in doc:
                    relations = doc['citations']
                    extended_relations = self.process_relations(relations, extended_relations)

                if 'relations' in doc:
                    relations = doc['relations']
                    extended_relations = self.process_relations(relations, extended_relations)
        else:
            self.missing_pubmed_ids.append(pmid)
            # print 'Pubmed doc not found:', pmid

        return extended_relations

    def process_relations(self, relations, extended_relations):
        for relation in relations:
            index_id = relation['index_id']
            if index_id in RELATION_INDEXES:
                ids = relation['ids']
                if index_id not in extended_relations:
                    extended_relations[index_id] = {}

                for _id in ids:
                    if _id not in extended_relations[index_id]:
                        extended_relations[index_id][_id] = ''

        return extended_relations


def process_relations(batch_file_name):
    load_config = get_load_config()

    relations_processor = RelationsProcessor(load_config, batch_file_name, session)
    relations_processor.process_irdb_relations()


class BatchRelationsProcessor:
    def __init__(self, load_config):
        self.load_config = load_config
        self.pool_count = PROCESS_COUNT

    def start(self):
        generated_files_directory = self.load_config.data_source_directory()

        batch_file_names = []
        for batch_file_name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        print "Generated ", len(batch_file_names), 'batch file names'

        batch_file_names.sort()

        p = Pool(self.pool_count)
        p.map(process_relations, batch_file_names)



def run():
    load_config = get_load_config()

    # filter_and_split_ids_into_batches(load_config)

    batch_relations_processor = BatchRelationsProcessor(load_config)
    batch_relations_processor.start()

import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'
    
# run()
