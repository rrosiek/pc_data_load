
import os
import json
import sys

import data_load.base.utils.file_utils as file_utils

from data_load.base.constants import *
from config import ROOT_DIRECTORY

generated_files_directory = ROOT_DIRECTORY + '/' + GENERATED_FILES_DIRECTORY


def analyse_all():
    folder_names = []
    for name in os.listdir(generated_files_directory):
        folder_names.append(name)
    folder_names.sort()

    for name in folder_names:
        analyse_docs(name)


def analyse_docs(name):
    all_updated_ids = {}
    all_failed_ids = {}
    all_skipped_ids = {}

    print 'Analysing', name
    root_dir = os.path.join(generated_files_directory, name)
    for data_source_batch_name in os.listdir(root_dir):
        data_source_batch_dir = os.path.join(root_dir, data_source_batch_name)
        if not os.path.isfile(data_source_batch_dir) and data_source_batch_name.startswith(DATA_SOURCE_BATCH_PREFIX) and 'old' not in data_source_batch_name:
            updated_ids, failed_ids, skipped_ids = analyse_docs_in_batch(data_source_batch_dir)
            print '..................................................................................'
            print 'Data source batch:', data_source_batch_name
            print 'Updated ids:', len(updated_ids)
            print 'Failed ids:', len(failed_ids)
            print 'Skipped ids:', len(skipped_ids)

            for _id in updated_ids:
                all_updated_ids[_id] = 0

            for _id in failed_ids:
                all_failed_ids[_id] = 0

            for _id in skipped_ids:
                all_skipped_ids[_id] = 0

    print 'Done analysing', name

    print '========================================================================'
    print name
    print 'Total updated ids:', len(all_updated_ids)
    print 'Total failed ids:', len(all_failed_ids)
    print 'Total skipped ids:', len(all_skipped_ids)
    print '========================================================================'
    print '.'


def analyse_docs_in_batch(data_loader_batch_directory):
    all_updated_ids = {}
    all_failed_ids = {}
    all_skipped_ids = {}

    # print '...Processing', data_loader_batch_directory
    for name in os.listdir(data_loader_batch_directory):
        file_path = os.path.join(data_loader_batch_directory, name)
        if os.path.isfile(file_path) and name.startswith(DATA_LOADER_BATCH_PREFIX):
            data_loader_batch = file_utils.load_file(data_loader_batch_directory, name)
            updated_ids = data_loader_batch['updated_ids']
            failed_ids = data_loader_batch['failed_ids']
            skipped_ids = data_loader_batch['skipped_ids']

            for _id in updated_ids:
                all_updated_ids[_id] = 0

            for _id in failed_ids:
                all_failed_ids[_id] = 0

            for _id in skipped_ids:
                all_skipped_ids[_id] = 0

    return all_updated_ids, all_failed_ids, all_skipped_ids

if __name__ == "__main__":
    print "This is the name of the script: ", sys.argv[0]
    print "Number of arguments: ", len(sys.argv)
    print "The arguments are: ", str(sys.argv)

    if len(sys.argv) >= 2:
        name = sys.argv[1]
        analyse_docs(name)
    else:
        analyse_all()
