from datetime import datetime, timedelta
import zipfile
import os
import urllib

from data_load.base.utils import file_utils

GRANTS_XML_EXTRACT_URL = 'https://www.grants.gov/extract/'
GRANTS_XML_EXTRACT_FILE_NAME_PREFIX = 'GrantsDBExtract'
GRANTS_PROCESSED_FILES = 'GRANTS_PROCESSED_FILES.json'
GRANTS_DOWNLOADED_FILES = 'GRANTS_DOWNLOADED_FILES.json'

# GrantsDBExtract20181203v2.zip

def get_available_files_to_download():
    urls = []
    file_names = get_filenames_for_last_seven_days()
    for file_name in file_names:
        url = GRANTS_XML_EXTRACT_URL + file_name
        urls.append(url) 

    return urls   

def get_available_files_to_process(load_config):
    source_files_directory = load_config.source_files_directory()

    source_files = []
    for name in os.listdir(source_files_directory):
        file_path = os.path.join(source_files_directory, name)
        source_files.append(file_path)

    return source_files

def get_downloaded_files(load_config):
    other_files_directory = load_config.other_files_directory()
    downloaded_files = file_utils.load_file(other_files_directory, GRANTS_DOWNLOADED_FILES)
    if len(downloaded_files) == 0:
        downloaded_files = []
    return downloaded_files

def set_downloaded_files(load_config, downloaded_files):
    other_files_directory = load_config.other_files_directory()
    file_utils.save_file(other_files_directory, GRANTS_DOWNLOADED_FILES, downloaded_files)

def get_processed_files(load_config):
    other_files_directory = load_config.other_files_directory()
    processed_files = file_utils.load_file(other_files_directory, GRANTS_PROCESSED_FILES)
    return processed_files

def set_processed_files(load_config, processed_files):
    other_files_directory = load_config.other_files_directory()
    file_utils.save_file(other_files_directory, GRANTS_PROCESSED_FILES, processed_files)

def get_files_to_process(load_config):
    available_files = get_available_files_to_process(load_config)
    processed_files = get_processed_files(load_config)

    filtered_files = []
    for available_file in available_files:
        if available_file not in processed_files:
            filtered_files.append(available_file)

    filtered_files.sort()
    return filtered_files

def get_files_to_download(load_config):
    available_files = get_available_files_to_download()
    downloaded_files = get_downloaded_files(load_config)

    filtered_files = []
    for available_file in available_files:
        if available_file not in downloaded_files:
            filtered_files.append(available_file)

    filtered_files.sort()
    return filtered_files


def download_files(load_config):
    files_to_download = get_files_to_download(load_config)
    source_files_directory = load_config.source_files_directory()

    downloaded_update_file_urls = get_downloaded_files(load_config)
    downloaded_update_file_paths = []

    print 'Downloading', len(files_to_download), 'files...'
    for update_file_url in files_to_download:
        file_name = os.path.basename(update_file_url)
        update_file_path = os.path.join(source_files_directory, file_name)
        xml_file_path = os.path.join(source_files_directory, file_name.replace('.zip', '.xml'))

        # Download update zip file
        urllib.urlcleanup()
        print 'Downloading file: ', update_file_url
        urllib.urlretrieve(update_file_url, update_file_path)
        print 'Saved', update_file_path

        # TODO - Verify download with md5?

        # Extract update zip file

        print 'Unzipping file', update_file_path
        try:
            with zipfile.ZipFile(update_file_path, 'r') as zip_ref:
                zip_ref.extractall(source_files_directory)

            downloaded_update_file_urls.append(update_file_url)
            downloaded_update_file_paths.append(xml_file_path)
        except Exception as e:
            print e

        # f = gzip.open(update_file_path, 'rb')
        # with open(xml_file_path, 'w') as xml_file:
        #     xml_file.write(f.read())
        # f.close()

        # Delete update zip file
        print 'Deleting file', update_file_path
        os.remove(update_file_path)

        # Save the downloaded files list
        set_downloaded_files(load_config, downloaded_update_file_urls)

    return downloaded_update_file_paths

def get_file_name(now):
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    return GRANTS_XML_EXTRACT_FILE_NAME_PREFIX + year + month + day + 'v2.zip'

def get_filenames_for_last_seven_days():
    days_to_subtract = 0
    file_names = []
    while days_to_subtract <= 7:
        d = datetime.today() - timedelta(days=days_to_subtract)
        file_name = get_file_name(d)
        file_names.append(file_name)
        days_to_subtract += 1

    return file_names

