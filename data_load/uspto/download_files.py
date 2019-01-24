
import os

import get_data_source_links
from data_load.base.utils import file_utils
from datetime import datetime, timedelta
import zipfile
import os
import urllib

USPTO_DOWNLOADED_FILES = 'USPTO_DOWNLOADED_FILES.json'

source_files_directory = '/data/data_loading/source-files/uspto/'


def load_files_per_year():
    file_utils.make_directory(source_files_directory)
    return file_utils.load_file(source_files_directory, 'files_per_year.json')

def save_files_per_year(files_per_year):
    file_utils.make_directory(source_files_directory)
    file_utils.save_file(source_files_directory, 'files_per_year.json', files_per_year)

def get_available_files_to_download(year=None):
    files_per_year = load_files_per_year()

    if files_per_year == None or len(files_per_year) == 0:
        files_per_year = get_data_source_links.run()
        save_files_per_year(files_per_year)

    print len(files_per_year)
    if year is not None:
        urls = files_per_year[year]
    else:
        urls = []
        years = files_per_year.keys()
        years.sort()

        for year in years:
            urls.extend(files_per_year[year])

    print urls
    return urls


def get_downloaded_files(other_files_directory):
    downloaded_files = file_utils.load_file(other_files_directory, USPTO_DOWNLOADED_FILES)
    if len(downloaded_files) == 0:
        downloaded_files = []
    return downloaded_files

def set_downloaded_files(other_files_directory, downloaded_files):
    file_utils.save_file(other_files_directory, USPTO_DOWNLOADED_FILES, downloaded_files)


def download_files(year=None):
    files_to_download = get_available_files_to_download(year=year)
    file_utils.make_directory(source_files_directory)

    downloaded_update_file_urls = get_downloaded_files(source_files_directory)
    downloaded_update_file_paths = []

    print 'Downloading', len(files_to_download), 'files...'
    for update_file_url in files_to_download:
        if update_file_url not in downloaded_update_file_urls:
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
                set_downloaded_files(source_files_directory, downloaded_update_file_urls)

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
    set_downloaded_files(source_files_directory, downloaded_update_file_urls)

    return downloaded_update_file_paths

def run():
    year = 2019
    while year >= 2001:
        download_files(str(year))
        year = year - 1

run()