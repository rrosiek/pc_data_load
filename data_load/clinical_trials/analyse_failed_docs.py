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
        analyse_failed_docs(name)


def analyse_failed_docs(name):
    print 'Analysing', name
    root_dir = os.path.join(generated_files_directory, name)
    for data_source_batch_name in os.listdir(root_dir):
        data_source_batch_dir = os.path.join(root_dir, data_source_batch_name)
        if not os.path.isfile(data_source_batch_dir) and data_source_batch_name.startswith(DATA_SOURCE_BATCH_PREFIX) and 'old' not in data_source_batch_name:
            analyse_failed_docs_in_batch(data_source_batch_dir)


def analyse_failed_docs_in_batch(data_loader_batch_directory):
    # print '...Processing', data_loader_batch_directory
    for name in os.listdir(data_loader_batch_directory):
        file_path = os.path.join(data_loader_batch_directory, name)
        if os.path.isfile(file_path) and name.startswith("failed_docs_"):
            failed_docs = file_utils.load_file(data_loader_batch_directory, name)
            print file_path, '- Failed docs', len(failed_docs)
            if len(failed_docs) > 0:
                a = raw_input('List docs? (y/n)')
                if a.lower() in ['y', 'yes']:
                    for _id in failed_docs:
                        reason = failed_docs[_id]['reason']
                        print 'Doc:', _id
                        print 'Reason', reason
                        c = raw_input('Continue?')
                        if c.lower() in ['n', 'no']:
                            break


if __name__ == "__main__":
    print "This is the name of the script: ", sys.argv[0]
    print "Number of arguments: ", len(sys.argv)
    print "The arguments are: ", str(sys.argv)

    if len(sys.argv) >= 2:
        name = sys.argv[1]
        analyse_failed_docs(name)
    else:
        analyse_all()
