
import data_load.base.utils.file_utils as file_utils
import os
PROCESSED_UPDATE_FILES = 'processed_update_files.json'


def get_baseline_files(load_config, baseline_file_urls):
    source_files_directory = load_config.source_files_directory()
    baseline_files = []

    for baseline_file_url in baseline_file_urls:
        file_name = os.path.basename(baseline_file_url)
        xml_file_path = os.path.join(source_files_directory, file_name.replace('.gz', ''))
        baseline_files.append(xml_file_path)

    source_files = get_all_files(load_config)
    available_files = []

    for file_path in source_files:
        if file_path in baseline_files:
            available_files.append(file_path)

    available_files.sort()
    return available_files

def get_new_update_files(load_config, update_file_urls, count=0):
    source_files_directory = load_config.source_files_directory()

    update_files = []
    for update_file_url in update_file_urls:
        file_name = os.path.basename(update_file_url)
        xml_file_path = os.path.join(source_files_directory, file_name.replace('.gz', ''))
        update_files.append(xml_file_path)

    new_files = get_new_files(load_config)
    available_files = []

    for file_path in new_files:
        if file_path in update_files:
            available_files.append(file_path)

    available_files.sort()

    new_update_files = []
    max_length = min(len(available_files), count)
    # print max_length, 'max_length'
    for i in range(0, max_length):
        new_update_file = available_files[i]
        new_update_files.append(new_update_file)

    return new_update_files

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

def set_processed_files(load_config, processed_file_urls):
    other_files_directory = load_config.other_files_directory()
    file_utils.save_file(other_files_directory,
                            PROCESSED_UPDATE_FILES, processed_file_urls)


def update_processed_files(load_config, file_urls):
    # Update processed update files
    processed_file_urls = get_processed_files(load_config)
    # print processed_file_urls
    # print file_urls
    processed_file_urls.extend(file_urls)
    set_processed_files(load_config, processed_file_urls)
