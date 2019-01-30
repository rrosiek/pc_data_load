
import data_load.base.utils.file_utils as file_utils
import data_load.base.utils.data_utils as data_utils

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
    load_config.data_source_name = DS_EXTENDED_RELATIONS

    generated_files_directory = load_config.generated_files_directory()

    batch_file_names = []
    for batch_file_name in os.listdir(generated_files_directory):
        file_path = os.path.join(generated_files_directory, batch_file_name)
        if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
            batch_file_names.append(batch_file_name)

    for batch_file_name in batch_file_names:
        batch = file_utils.load_file(generated_files_directory, batch_file_name)
        if len(batch) < 1000:
            print batch_file_name, len(batch)

run()