
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


def run():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DS_PATENT_RELATIONS
    data_source_batch_name = 'loaded_ids'

    data_source_batch_directory = load_config.data_source_batch_directory(data_source_batch_name)
    
    all_updated_ids = {}
    all_indexed_ids = {}
    all_failed_ids = {}

    for name in os.listdir(data_source_batch_directory):
        file_path = os.path.join(data_source_batch_directory, name)
        if os.path.isfile(file_path) and name.startswith(DATA_LOADER_BATCH_PREFIX):
            # print 'processing file:', file_path
            batch_data = file_utils.load_file(data_source_batch_directory, name)
            updated_ids = batch_data['updated_ids']
            indexed_ids = batch_data['indexed_ids']
            failed_ids = batch_data['failed_ids']

            for _id in updated_ids:
                all_updated_ids[_id] = 0

            for _id in indexed_ids:
                all_indexed_ids[_id] = 0

            for _id in failed_ids:
                all_failed_ids[_id] = 0

    print len(all_failed_ids), 'all_failed_ids'
    print len(all_indexed_ids), 'all_indexed_ids'
    print len(all_updated_ids), 'all_updated_ids'

def process_file(load_config, batch_file_name):
    generated_files_directory = load_config.generated_files_directory()
    # print 'Processing batch', batch_file_name

    processes_ids_file_name = 'processed_ids_' + batch_file_name
    ids_to_update_file_name = 'ids_to_update_' + batch_file_name

    # Get processed ids
    processed_ids = file_utils.load_file(generated_files_directory, processes_ids_file_name)
    if processed_ids is None or len(processed_ids) == 0:
        print 'Processed ids file not found, aborting...'
        return

    # Get batch ids
    batch_ids = file_utils.load_file(generated_files_directory, batch_file_name)
    if batch_ids is None or len(batch_ids) == 0:
        print 'batch ids not found, aborting....'
        return

    # Continue processing
    # print batch_file_name, 'Processed ids count:', len(processed_ids), ' ~ ', len(batch_ids)
    
    # if len(processed_ids) != len(batch_ids):
    #     print 'Processing not finished, aborting...'
    #     return
    # else:
    #     print 'Processing complete for', self.batch_file_name, ', proceeding with data load...'

    ids_to_update = file_utils.load_file(generated_files_directory, ids_to_update_file_name)
    total_count = len(ids_to_update)
    count = 0

    reformatted_array = {}
    for _id in ids_to_update:
        count += 1
        derwent_ids = ids_to_update[_id]
        
        if _id not in reformatted_array:
            reformatted_array[_id] = []

        if len(derwent_ids) > 0:
            relationship = {
                'index_id': ID_DERWENT_PATENTS,
                'ids': derwent_ids,
                'type': RELATIONSHIP_TYPE_RELATIONS
            }

            reformatted_array[_id].append(relationship)



    # print 'Reformatted ids', len(reformatted_array)

    return reformatted_array, ids_to_update

# def docs_fetched_irdb()

def analyse_batches():
    load_config = irdb_load_config.get_load_config()
    load_config.data_source_name = DS_PATENT_RELATIONS
    generated_files_directory = load_config.generated_files_directory()

    batch_file_names = []
    for batch_file_name in os.listdir(generated_files_directory):
        file_path = os.path.join(generated_files_directory, batch_file_name)
        if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
            batch_file_names.append(batch_file_name)

    print "Generated ", len(batch_file_names), 'batch file names'

    all_ids = {}
    batches_with_no_ids = {}
    ids_with_more_rels = {}

    for batch_file_name in batch_file_names:
        reformatted_array, ids_to_update = process_file(load_config, batch_file_name)
        print batch_file_name, 'reformatted ids', len(reformatted_array)
        if len(reformatted_array) == 0:
            batches_with_no_ids[batch_file_name] = len(ids_to_update)
        for _id in reformatted_array:
            if _id not in all_ids:
                all_ids[_id] = len(reformatted_array[_id])
            
            indexes = {}
            for item in reformatted_array[_id]:
                index_id = item['index_id']
                if index_id not in indexes:
                    indexes[index_id] = 0
                else:
                    'print duplicate index_id'
                    ids_with_more_rels[_id] = index_id



    print len(all_ids), 'all_ids'
    print 'ids_with_more_rels', ids_with_more_rels

    print 'batches_with_no_ids'
    print batches_with_no_ids

    run()

    # data_utils.batch_fetch_docs_for_ids(LOCAL_SERVER,
    #                                     ids,
    #                                     INDEX_MAPPING[ID_IRDB]['index'],
    #                                     INDEX_MAPPING[ID_IRDB]['type'],
    #                                     self.docs_fetched_irdb, 1000)

                                

analyse_batches()


