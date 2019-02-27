from datetime import datetime, timedelta
import zipfile
import os
import urllib

from data_load.base.utils import file_utils
import get_data_source_links

GRANTS_PROCESSED_FILES = 'USPTO_PROCESSED_FILES.json'
GRANTS_DOWNLOADED_FILES = 'USPTO_DOWNLOADED_FILES.json'

# GrantsDBExtract20181203v2.zip

def get_available_files_to_download(year=None, pre_2001=False):
    files_per_year = get_data_source_links.get_files(pre_2001=pre_2001)

    print len(files_per_year)
    if year is not None:
        urls = files_per_year[year]
    else:
        urls = []
        years = files_per_year.keys()
        years.sort(reverse=False)

        for year in years:
            urls.extend(files_per_year[year])

    print urls
    return urls   

def get_next_year(load_config, pre_2001=False):
    files_per_year = get_data_source_links.get_files(pre_2001=pre_2001)

    downloaded_files = get_downloaded_files(load_config)

    years = files_per_year.keys()
    years.sort(reverse=False)
    for year in years:
        files_to_download = files_per_year[year]

        for file_url in files_to_download:
            if file_url not in downloaded_files:
                return year

    return None

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

    filtered_files.sort(reverse=False)
    return filtered_files

def get_files_to_download(load_config, available_files):
    # available_files = get_available_files_to_download(year=year)
    downloaded_files = get_downloaded_files(load_config)

    filtered_files = []
    for available_file in available_files:
        if available_file not in downloaded_files:
            filtered_files.append(available_file)

    filtered_files.sort(reverse=False)
    return filtered_files


def download_files(load_config, year=None, pre_2001=False):
    available_files_to_download = get_available_files_to_download(year=year, pre_2001=pre_2001)

    files_to_download = get_files_to_download(load_config, available_files_to_download)
    source_files_directory = load_config.source_files_directory()

    downloaded_update_file_urls = get_downloaded_files(load_config)
    downloaded_update_file_paths = []

    print 'Downloading', len(files_to_download), 'files...'
    for update_file_url in available_files_to_download:
        file_name = os.path.basename(update_file_url)
        update_file_path = os.path.join(source_files_directory, file_name)
        xml_file_path = os.path.join(source_files_directory, file_name.replace('.zip', '.xml'))

        if update_file_url in files_to_download:
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
        else:
            downloaded_update_file_paths.append(xml_file_path)

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

