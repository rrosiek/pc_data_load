from ftplib import FTP

import urllib
import gzip
import os

import data_load.base.utils.file_utils as file_utils
from config import PROCESSED_UPDATE_FILES
from data_load.DATA_LOAD_CONFIG import PUBMED_FTP_URL,  PUBMED_UPDATES_DIRECTORY,  PUBMED_BASELINE_DIRECTORY

DOWNLOADED_UPDATE_FILES = 'downloaded_update_files.json'


class FTPManager(object):

    def __init__(self, load_config):
        self.load_config = load_config

    def download_new_update_files(self):
        update_file_urls = self.get_update_file_urls()
        filtered_update_file_urls = self.filter_update_file_urls(update_file_urls)
        self.download_update_files(filtered_update_file_urls)

    def download_n_files(self, no_of_files):
        file_urls = self.get_baseline_file_urls()
        file_urls.extend(self.get_update_file_urls())
        filtered_file_urls = self.filter_update_file_urls(file_urls)

        if len(filtered_file_urls) > no_of_files:
            filtered_file_urls = filtered_file_urls[:no_of_files]

        self.download_update_files(filtered_file_urls[:no_of_files])

    def get_xml_file_urls_from_directory(self, directory):
        print 'Fetching files list:', PUBMED_FTP_URL + '/' + directory
        ftp = FTP()
        ftp.connect(PUBMED_FTP_URL)
        ftp.login()
        files = ftp.nlst(directory)
        xml_zip_files = []
        for f in files:
            if f.endswith('.xml.gz'):
                abs_url = 'ftp://' + PUBMED_FTP_URL + '/' + f
                xml_zip_files.append(abs_url)
                # print abs_url

        print len(xml_zip_files), 'files'
        return xml_zip_files

    def get_update_file_urls(self):
        return self.get_xml_file_urls_from_directory(PUBMED_UPDATES_DIRECTORY)

    def get_baseline_file_urls(self):
        return self.get_xml_file_urls_from_directory(PUBMED_BASELINE_DIRECTORY)

    def filter_update_file_urls(self, update_file_urls):
        # Filter update files list from downloaded files list
        downloaded_update_file_urls = self.get_downloaded_update_file_urls()

        filtered_update_file_urls = []
        for update_file_url in update_file_urls:
            if update_file_url not in downloaded_update_file_urls:
                filtered_update_file_urls.append(update_file_url)

        filtered_update_file_urls.sort()
        return filtered_update_file_urls

    def download_update_files(self, update_file_urls):
        """
        Download new update files - update_file_urls
        Save list of downloaded file urls to file
        """
        source_files_directory = self.load_config.source_files_directory()

        # Get the downloaded files list
        downloaded_update_file_urls = self.get_downloaded_update_file_urls()

        # Download new update zip files, extract them and delete zip files
        for update_file_url in update_file_urls:
            file_name = os.path.basename(update_file_url)
            update_file_path = os.path.join(source_files_directory, file_name)
            xml_file_path = os.path.join(source_files_directory, file_name.replace('.gz', ''))

            # Download update zip file
            urllib.urlcleanup()
            print 'Downloading file: ', update_file_url
            urllib.urlretrieve(update_file_url, update_file_path)
            print 'Saved', update_file_path

            # TODO - Verify download with md5?

            # Extract update zip file
            f = gzip.open(update_file_path, 'rb')
            print 'Unzipping file', update_file_path
            with open(xml_file_path, 'w') as xml_file:
                xml_file.write(f.read())
            f.close()

            # Delete update zip file
            print 'Deleting file', update_file_path
            os.remove(update_file_path)

            downloaded_update_file_urls.append(update_file_url)

        # Save the downloaded files list
        self.set_downloaded_update_file_urls(downloaded_update_file_urls)

    def get_downloaded_update_file_urls(self):
        other_files_directory = self.load_config.other_files_directory()
        downloaded_update_file_urls = file_utils.load_file(other_files_directory, DOWNLOADED_UPDATE_FILES)
        if len(downloaded_update_file_urls) == 0:
            return []
        return downloaded_update_file_urls

    def set_downloaded_update_file_urls(self, downloaded_update_file_urls):
        other_files_directory = self.load_config.other_files_directory()
        file_utils.save_file(other_files_directory, DOWNLOADED_UPDATE_FILES, downloaded_update_file_urls)
