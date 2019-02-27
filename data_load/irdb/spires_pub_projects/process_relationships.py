__author__ = 'Robin'
import json
import csv

from data_load.irdb.config import *
import data_load.irdb.irdb_load_config as irdb_load_config


from data_load.base.utils.batch_doc_processor import BatchDocProcessor

import data_load.base.utils.export_doc_ids as export_doc_ids
from data_load.base.utils.data_utils import DataUtils
import data_load.base.utils.file_utils as file_utils
from data_load.base.utils.download_data import CSVDownloader

# SERVER = 'http://localhost:9200/'

# INDEX = "irdb"
# TYPE = "grant"

# ID_FIELD = 'appl_id'
# ID_PREFIX = 'IRDB'

# DATA_DIRECTORY = "/data/data_loading/irdb/"
RELATIONSHIPS_FILE = "spires_pub_projects.csv"
# RELATIONSHIPS_FILE = "spires_pub_projects_jan_4_2017.csv"

FULL_PROJECT_NUM__PMID = "FULL_PROJECT_NUM__PMID.json"
GRANT_NUM__GROUPS = 'GRANT_NUM__GROUPS.json'

class GenerateRelationships(object):

    def __init__(self):
        self.grant_num_groups = {}

    def get_load_config(self):
        load_config = irdb_load_config.get_load_config()
        load_config.data_source_name = DS_SPIRES_PUB_PROJECTS
        # load_config.log_level = LOG_LEVEL_TRACE

        load_config.source = 'irdb'
        load_config.append_relations = False

        load_config.auto_retry_load = True
        load_config.max_retries = 2
        return load_config

    def download_spires_pub_projects(self):
        # print('Downloading spires_pub_projects csv...')
        # load_config = self.get_load_config()
        # other_files_directory = load_config.other_files_directory()
        # csv_downloader = CSVDownloader(other_files_directory)
        # csv_downloader.download('spires_pub_projects')
        pass

    def fetch_irdb_docs(self):
        print('Fetching irdb docs...')

        load_config = self.get_load_config()
        other_files_directory = load_config.other_files_directory()
        document_ids = export_doc_ids.get_doc_ids_for_load_config(load_config)
        # document_ids = get_doc_ids(server=load_config.server,
        #                             src_index=load_config.index,
        #                             src_type=load_config.type,
        #                             dest_dir=other_files_directory,
        #                             dest_file_name="all_irdb_ids.json")

        doc_ids = document_ids.keys()

        data_utils = DataUtils()
        data_utils.batch_fetch_docs_for_ids(base_url=load_config.server,
                                            ids=doc_ids,
                                            index=load_config.index,
                                            type=load_config.type,
                                            docs_fetched=self.process_batch)

        print(str(len(self.grant_num_groups)) + ' total grant num groups')

        file_utils.save_file(other_files_directory, GRANT_NUM__GROUPS, self.grant_num_groups)

    def process_batch(self, docs, index, type):
        print('Processing irdb docs batch' + str(len(docs)))

        for doc in docs: 
            es_id = doc['_id']
            if '_source' in doc:
                source = doc['_source']
                if 'grant_num' in source:
                    grant_num = source['grant_num']
                    appl_id = source['appl_id']

                    grant_num_comps = grant_num.split('-')
                    if grant_num_comps[0] not in self.grant_num_groups:
                        self.grant_num_groups[grant_num_comps[0]] = []

                    self.grant_num_groups[grant_num_comps[0]].append(es_id)

    def process_full_project_num__pmid_relationships(self):

        # pmid to core_project_num mapping
        full_project_num_for_pmids = {}
        print('Processing spires_pub_projects file...')
        load_config = self.get_load_config()
        other_files_directory = load_config.other_files_directory()

        with open(other_files_directory + '/' + 'testcsvconverted.csv') as data_file:
            reader = csv.DictReader(data_file)
            # records = data['RECORDS']
            # print('Relationship records count: ' + str(len(records)))
            for row in reader:
                # print row
                full_project_num = str(row['core_project_num'])
                pmid = str(row['pmid'])

                # print full_project_num, pmid
                if full_project_num not in full_project_num_for_pmids:
                    full_project_num_for_pmids[full_project_num] = []

                if pmid not in full_project_num_for_pmids[full_project_num]:
                    full_project_num_for_pmids[full_project_num].append(pmid)

        print(str(len(full_project_num_for_pmids)) + ' full_project_num to process')

        file_utils.save_file(other_files_directory, FULL_PROJECT_NUM__PMID, full_project_num_for_pmids)

    def process_grants_relationships(self):
        print('Processing irdb relations...')
        load_config = self.get_load_config()
        other_files_directory = load_config.other_files_directory()
        full_proj_num__pmid__mapping = file_utils.load_file(other_files_directory, FULL_PROJECT_NUM__PMID)
        grant_num_groups =  file_utils.load_file(other_files_directory, GRANT_NUM__GROUPS)

        matches = []

        appl_id__pmid__mapping = {}
        pmid__appl_id__mapping = {}

        for grant_num in grant_num_groups:
            if grant_num in full_proj_num__pmid__mapping:
                # print 'Found match', grant_num
                matches.append(grant_num)

                if len(matches) % 1000 == 0:
                    print 'Matches found', len(matches)

                pmid_mapping = full_proj_num__pmid__mapping[grant_num]

                for _id in grant_num_groups[grant_num]:
                    # print 'Processing id', _id
                    if _id not in appl_id__pmid__mapping:
                        appl_id__pmid__mapping[_id] = []

                    for pmid in pmid_mapping:
                        # print 'Processing pmid', pmid
                        if pmid not in appl_id__pmid__mapping[_id]:
                            appl_id__pmid__mapping[_id].append(pmid)

                        if pmid not in pmid__appl_id__mapping:
                            pmid__appl_id__mapping[pmid] = []

                        pmid__appl_id__mapping[pmid].append(_id)

        print len(appl_id__pmid__mapping), 'appl_id__pmid__mapping'
        print len(pmid__appl_id__mapping), 'pmid__appl_id__mapping'

        print len(full_proj_num__pmid__mapping), 'full_proj_num__pmid__mapping'
        print len(grant_num_groups), 'grant_num_groups'
        print len(matches), 'matches'

        file_utils.pickle_file(other_files_directory, 'appl_id__pmid__mapping.json', appl_id__pmid__mapping)
        file_utils.pickle_file(other_files_directory, 'pmid__appl_id__mapping.json', pmid__appl_id__mapping)

        # with open(DATA_DIRECTORY + 'appl_id__pmid__mapping.json', 'w') as save_file:
        #     save_file.write(json.dumps(appl_id__pmid__mapping))

        # with open(DATA_DIRECTORY + 'pmid__appl_id__mapping.json', 'w') as save_file:
        #     save_file.write(json.dumps(pmid__appl_id__mapping))

        print('Done')

    def run(self):
        self.download_spires_pub_projects()
        self.fetch_irdb_docs()
        self.process_full_project_num__pmid_relationships()
        self.process_grants_relationships()

def run():
    generate_relationships = GenerateRelationships()
    generate_relationships.run()


import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'
    
# run()

