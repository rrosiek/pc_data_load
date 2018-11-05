import pubmed_load_config
import data_load.base.utils.file_utils as file_utils

from config import *

def set_processed_update_files(load_config, processed_file_urls):
    other_files_directory = load_config.other_files_directory()
    file_utils.save_file(other_files_directory,
                            PROCESSED_UPDATE_FILES, processed_file_urls)


def get_processed_update_files(load_config):
    other_files_directory = load_config.other_files_directory()
    processed_file_urls = file_utils.load_file(
        other_files_directory, PROCESSED_UPDATE_FILES)
    if len(processed_file_urls) == 0:
        return []

    return processed_file_urls


def run():
    load_config = pubmed_load_config.get_load_config()
    processed_file_urls = get_processed_update_files(load_config)

    count = 0
    for processed_file in processed_file_urls:
        count += 1
        print count,  ':',  processed_file
        

    remove_last_file = raw_input('Remove last file?')
    if remove_last_file.lower() in ['y', 'yes']:
        removed_item = processed_file_urls.pop(len(processed_file_urls) - 1)

        count = 0
        for processed_file in processed_file_urls:
            count += 1
            print count,  ':',  processed_file

        print 'Removed Item:', removed_item

        save_file = raw_input('Save file?')
        if save_file.lower() in ['y', 'yes']:
            set_processed_update_files(load_config, processed_file_urls)


run()