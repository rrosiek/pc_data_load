
import data_load.base.utils.file_utils as file_utils
import os
PROCESSED_UPDATE_FILES = 'processed_update_files.json'




def get_new_files(load_config):
    processed_update_files = get_processed_files(load_config)
    source_files = get_all_files(load_config)
    new_update_files = []

    for file_path in source_files:
        if file_path not in processed_update_files:
            new_update_files.append(file_path)

    new_update_files.sort()
    return new_update_files


def get_all_files(load_config):
    source_files = []
    source_files_directory = load_config.source_files_directory()
    for name in os.listdir(source_files_directory):
        source_files.append(name)

    source_files.sort()
    source_file_paths = []
    for name in source_files:
        file_path = os.path.join(source_files_directory, name)
        if os.path.isfile(file_path) and name.endswith('.xml'):
            source_file_paths.append(file_path)

    return source_file_paths


def get_processed_files(load_config):
    other_files_directory = load_config.other_files_directory()
    processed_file_urls = file_utils.load_file(
        other_files_directory, PROCESSED_UPDATE_FILES)
    if len(processed_file_urls) == 0:
        return []

    return processed_file_urls