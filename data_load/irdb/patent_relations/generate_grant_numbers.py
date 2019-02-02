
import data_load.base.utils.file_utils as file_utils
from data_load.base.utils.data_utils import DataUtils

from data_load.base.utils import export_doc_ids
from data_load.base.constants import *
from data_load.irdb.config import *
import data_load.irdb.irdb_load_config as irdb_load_config


import json
import requests
import os
import time

from multiprocessing import Pool


IRDB_IDS_FILE_NAME = 'DOC_IDS_' + ID_IRDB + '.json'

def get_load_config():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DS_PATENT_RELATIONS
    return load_config

# def fetch_all_ids_from_index(load_config):
#     other_files_directory = load_config.other_files_directory()

#     export_doc_ids.export_doc_ids(server=LOCAL_SERVER,
#                                   src_index=INDEX_MAPPING[ID_IRDB]['index'],
#                                   src_type=INDEX_MAPPING[ID_IRDB]['type'],
#                                   dest_dir=other_files_directory,
#                                   dest_file_name=IRDB_IDS_FILE_NAME)


def filter_and_split_ids_into_batches(load_config):
    other_files_directory = load_config.other_files_directory()
    generated_files_directory = load_config.data_source_directory()

    all_ids = export_doc_ids.get_doc_ids_for_load_config(load_config)
    # total_count = len(all_ids)

    max_batch_count = 1000

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
        data_utils = DataUtils(self.session)
        if index_id == ID_IRDB:
            data_utils.batch_fetch_docs_for_ids(LOCAL_SERVER,
                                                ids,
                                                INDEX_MAPPING[index_id]['index'],
                                                INDEX_MAPPING[index_id]['type'],
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
        all_ids = file_utils.load_file(generated_files_directory, self.batch_file_name)

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

            if processed_count % 500 == 0:
                print 'Processing irdb', _id

            # print 'Processing', processed_count, '/', total_count
            derwent_ids = self.process_id(_id)
            if len(derwent_ids) > 0:
                # print ex_rl
                ids_to_update[_id] = derwent_ids

            if batch_count >= 500:
                batch_count = 0

                file_utils.save_file(generated_files_directory, 'ids_to_update_' + self.batch_file_name, ids_to_update)
                file_utils.save_file(generated_files_directory, 'processed_ids_' + self.batch_file_name, processed_ids)

        file_utils.save_file(generated_files_directory, 'ids_to_update_' + self.batch_file_name, ids_to_update)
        file_utils.save_file(generated_files_directory, 'processed_ids_' + self.batch_file_name, processed_ids)

        # file_utils.save_file(generated_files_directory, 'missing_pubmed_ids_' + self.batch_file_name, self.missing_pubmed_ids)

        print 'Docs to update..............................................', len(ids_to_update)


    def process_id(self, _id):
        grant_numbers = []
        derwent_ids = []
        if _id in self.irdb_docs:
            doc = self.irdb_docs[_id]
            if doc is not None:
                admin_phs_org_code = None
                if 'admin_phs_org_code' in doc:
                    admin_phs_org_code = doc['admin_phs_org_code']

                serial_num = None
                if 'serial_num' in doc:
                    serial_num = doc['serial_num']

                if admin_phs_org_code is not None and serial_num is not None:
                    grant_number = admin_phs_org_code + '' + serial_num
                    grant_numbers.append(grant_number)

                    grant_number = admin_phs_org_code + '-' + serial_num
                    grant_numbers.append(grant_number)

                    grant_number = admin_phs_org_code + '0' + serial_num
                    grant_numbers.append(grant_number)   
                    
                    grant_number = admin_phs_org_code + '-0' + serial_num
                    grant_numbers.append(grant_number)

                    grant_number = admin_phs_org_code + ' ' + serial_num
                    grant_numbers.append(grant_number)

                    grant_number = admin_phs_org_code + ' 0' + serial_num
                    grant_numbers.append(grant_number)

        if len(grant_numbers) > 0:
            should_query = []
            for grant_number in grant_numbers:
                match_phrase_query = {
                    "match_phrase": {
                        "government_support": grant_number
                    }
                }

                should_query.append(match_phrase_query)

            query = {
                "bool": {
                    "should": should_query
                }
            }

            data_utils = DataUtils(self.session)
            derwent_ids = data_utils.batch_fetch_ids_for_query(base_url=SERVER,
                                                               query=query,
                                                               index=INDEX_MAPPING[ID_DERWENT_PATENTS]['index'],
                                                               type=INDEX_MAPPING[ID_DERWENT_PATENTS]['type'])

            # if len(derwent_ids) > 0:
            #     print _id, len(derwent_ids)

            #     if len(derwent_ids) < 5:
            #         print derwent_ids
            #     time.sleep(5)

        return derwent_ids

def process_relations(batch_file_name):
    session = requests.Session()
    load_config = get_load_config()

    relations_processor = RelationsProcessor(load_config, batch_file_name, session)
    relations_processor.process_irdb_relations()

class BatchRelationsProcessor:
    def __init__(self, load_config):
        self.load_config = load_config
        self.pool_count = 1

    def start(self):
        generated_files_directory = self.load_config.data_source_directory()

        batch_file_names = []
        for batch_file_name in os.listdir(generated_files_directory):
            file_path = os.path.join(generated_files_directory, batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        print "Generated ", len(batch_file_names), 'batch file names'

        p = Pool(self.pool_count)
        p.map(process_relations, batch_file_names)

def run():
    load_config = get_load_config()
    load_config.data_source_name = 'patent_relations'

    # fetch_all_ids_from_index(load_config)
    filter_and_split_ids_into_batches(load_config)
    
    batch_relations_processor = BatchRelationsProcessor(load_config)
    batch_relations_processor.start()


import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'

# run()
