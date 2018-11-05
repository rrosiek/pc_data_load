from data_load.base.utils.download_data import CSVDownloader
from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_csv import CSVDataSource
from data_load.clinical_trials.ct_relationship_processor import CTRelationshipProcessor

import data_load.base.utils.file_utils as file_utils

import ct_load_config
from config import *

import requests
import json
import sys


def generate_queries(tables):
    ct_queries = {}
    for ct_table in tables:
        ct_query = """select * from """ + ct_table
        print ct_query
        ct_queries[ct_table] = ct_query

    return ct_queries


def process_file(data_source_name):
    print 'processing', data_source_name

    load_config = ct_load_config.get_load_config()
    load_config.data_source_name = data_source_name

    source_files_directory = load_config.source_files_directory()
    data_source_file_path = source_files_directory + '/' + data_source_name + '.csv'

    print 'Processing file', data_source_file_path
    # data_processor = DataProcessor(load_config, CSVDataSource(data_source_file_path))
    # data_processor.mode = DataProcessor.MODE_RETRY_FAILED_DOCS
    # data_processor.process_rows()

    data_processor = DataSourceProcessor(load_config, CSVDataSource(data_source_file_path))
    data_processor.run()

def process_relationships(data_source_name):
    print 'processing', data_source_name
    load_config = ct_load_config.get_load_config()
    load_config.data_source_name = data_source_name

    # Relationships
    load_config.append_relations = False
    load_config.source = data_source_name

    source_files_directory = load_config.source_files_directory()
    data_source_file_path = source_files_directory + '/' + data_source_name + '.csv'

    data_processor = CTRelationshipProcessor(load_config, CSVDataSource(data_source_file_path))
    # data_processor.mode = DataProcessor.MODE_RETRY_FAILED_DOCS
    data_processor.run()


def create_index():
    data_loader_utils = DataLoaderUtils(SERVER, INDEX, TYPE)
    data_loader_utils.check_and_create_index('data_load/clinical_trials/mapping.json')


def download_data():
    load_config = ct_load_config.get_load_config()
    source_files_directory = load_config.source_files_directory()

    csv_downloader = CSVDownloader(source_files_directory)
    for ct_table in CT_TABLES:
        csv_downloader.download(ct_table)


def get_processed_data_sources():
    load_config = ct_load_config.get_load_config()
    other_files_directory = load_config.other_files_directory()

    processed_data_sources = file_utils.load_file(other_files_directory, 'processed_data_sources.json')
    return processed_data_sources

def save_processed_data_sources(processed_data_sources):
    load_config = ct_load_config.get_load_config()
    other_files_directory = load_config.other_files_directory()

    file_utils.save_file(other_files_directory, 'processed_data_sources.json', processed_data_sources)

def load_stats(load_config, data_source_name):
    data_source_directory = load_config.data_source_directory(data_source_name)
    stats = file_utils.load_file(data_source_directory, 'stats.json')
    return stats

def verify():
    load_config = ct_load_config.get_load_config()

    for data_source in DATA_SOURCES:
        # print 'Verifying', data_source
        stats = load_stats(load_config, data_source)
        total_ids = 0
        if 'total_ids' in stats:
            total_ids = stats['total_ids']

        url = load_config.server + '/' + load_config.index + '/' + load_config.type + '/_search?size=0'
        data = {
            "query":{
                "exists":{
                    "field":data_source
                }
            }
        }

        response = requests.post(url, json=data) 
        # print response
        if response.status_code == 200:
            response_data = json.loads(response.text)
            if 'hits' in response_data:
                hits = response_data['hits']
                if 'total' in hits:
                    total = hits['total']
                    complete = ''
                    if total != total_ids:
                        complete = 'Incomplete'
                    print data_source, 'es_count:', total, 'stats_count:', total_ids, '[', complete, ']'
        else:
            print response.text

def run():
    # TODO Download csv from pardi database
    # download_data()

    # create_index()
    # process_file('ct_locations')

    # processed_data_sources = get_processed_data_sources()

    # for data_source in DATA_SOURCES:
    #     if data_source not in processed_data_sources:
    #         process_file(data_source)
    #         processed_data_sources[data_source] = 0
    #         save_processed_data_sources(processed_data_sources)


    # TODO clear relations
    process_relationships("ct_references")

        
# run()
# download_data()
# create_index()


# data_sources = [
#     "ct_clinical_studies",
#     "ct_arm_groups",
#     "ct_authorities",
#     "ct_collaborators",
#     "ct_condition_browses",
#     "ct_conditions",
#     "ct_intervention_arm_group_labels",
#     "ct_intervention_browses",
#     "ct_intervention_other_names",
#     "ct_interventions",
#     "ct_keywords",
#     "ct_links",
#     "ct_location_countries",
#     "ct_location_investigators",
#     "ct_locations",
#     "ct_outcomes",
#     "ct_overall_contacts",
#     "ct_overall_officials",
#     "ct_publications",
#     # "ct_pubmed_pmid",
#     "ct_references",
#     "ct_secondary_ids",
# ]

# data_sources = [
#     'ct_interventions',
#     'ct_overall_officials',
#     'ct_publications'
# ]

#
# # for data_source in data_sources:
# #     # raw_input('Load ' + data_source + '?')
# #     process_file(data_source)

# # process_file("ct_references")
# process_relationships("ct_references")


if __name__ == "__main__":
    if len(sys.argv) >= 2:  
        param_type = sys.argv[1]
        if param_type == '-verify':
            verify()
    else:
        run()            
        
