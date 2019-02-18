
from data_load.pubmed.ftp_manager import FTPManager
from data_load.base.data_source_xml import XMLDataSource
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor


from ftplib import FTP
import datetime

import urllib
import gzip
import os
import json
import data_load.base.utils.file_utils as file_utils

PUBMED_FTP_URL = 'ftp.ncbi.nlm.nih.gov'
PUBMED_UPDATES_DIRECTORY = 'pubmed/updatefiles/'
PUBMED_BASELINE_DIRECTORY = 'pubmed/baseline/'

class GetDoc(object):

    def __init__(self, pmids, files, directory):
        self.pmids = pmids
        self.files = files
        self.directory = directory
        self.extractor = PubmedDataExtractor()

    def run(self):
        file_paths = self.get_files()
        for file_path in file_paths:
            print 'Processing file', file_path
            xml_data_source = XMLDataSource(file_path, 2)
            xml_data_source.process_rows(self.process_row)

    def process_row(self, row, current_index):
        _id = self.extractor.extract_id('', row)

        if _id is not None:
            if current_index % 100 == 0:
                print _id
            if _id in self.pmids:
                doc = self.extractor.extract_data(_id, '', row)
                citations = self.get_citations([doc])
                print len(citations), _id

                print json.dumps(doc)
                raw_input('Continue?')
        return True

    def get_citations(self, doc):
        doc = self.get_latest_data_item(doc)

        citations = []
        if 'PubmedData' in doc:
            if 'ReferenceList' in doc['PubmedData']:
                if 'Reference' in doc['PubmedData']['ReferenceList']:
                    reference_list = doc['PubmedData']['ReferenceList']['Reference']

                    if not isinstance(reference_list, list):
                        reference_list = [reference_list]

                    for reference in reference_list:
                        if 'ArticleIdList' in reference:
                            article_id_list = reference['ArticleIdList']
                            if 'ArticleId' in article_id_list:
                                article_ids = article_id_list['ArticleId']
                                if not isinstance(article_ids, list):
                                    article_ids = [article_ids]

                                for article_id in article_ids:
                                    if 'IdType' in article_id:
                                        article_id_type = article_id['IdType']
                                        if article_id_type == 'pubmed':
                                            pmid = article_id['content']
                                            citations.append(pmid)

        return citations

    def get_date_revised(self, doc):
        date_revised = None
        if 'MedlineCitation' in doc:
            if 'DateRevised' in doc['MedlineCitation']:
                try:
                    dr = doc['MedlineCitation']['DateRevised']
                    year = int(dr['Year'])
                    month = int(dr['Month'])
                    day = int(dr['Day'])
                    date_revised = datetime.date(year=year, month=month, day=day)
                except Exception as e:
                    print 'Getting date revised', e
        return date_revised

    def get_latest_data_item(self, data):
        # for data_item in data:
        latest_date_revised = None
        latest_doc = None
        for doc in data:
            date_revised = self.get_date_revised(doc)
            if date_revised is not None:
                if latest_date_revised is None or date_revised >= latest_date_revised:
                    latest_date_revised = date_revised
                    latest_doc = doc
            
        if latest_doc is None:
            latest_doc = data[-1]

        return latest_doc

    def get_files(self):
        files_list = self.get_xml_file_urls_from_directory(PUBMED_BASELINE_DIRECTORY)
        files_list.extend(self.get_xml_file_urls_from_directory(PUBMED_UPDATES_DIRECTORY))

        downloaded_files = []

        existing_files = []
        for name in os.listdir(self.directory):
            file_path = os.path.join(self.directory, name)
            if os.path.isfile(file_path) and name.endswith('.xml'):
                existing_files.append(file_path)

        existing_file_names = []
        for file_name in self.files:
            for file_path in existing_files:
                if file_name in file_path:
                    downloaded_files.append(file_path)
                    existing_file_names.append(file_name)

        files_to_download = []
        for file_name in self.files:
            if file_name not in existing_file_names:
                for file_url in files_list:
                    if file_name in file_url:
                        files_to_download.append(file_url)

        downloaded_files.extend(self.download_files(files_to_download))

        return downloaded_files

    def download_files(self, update_file_urls):
        """
        Download new update files - update_file_urls
        Save list of downloaded file urls to file
        """
        # Get the downloaded files list
        downloaded_update_file_paths = []

        print 'Downloading', str(len(update_file_urls)), 'file(s)'
        # Download new update zip files, extract them and delete zip files
        for update_file_url in update_file_urls:
            file_name = os.path.basename(update_file_url)
            update_file_path = os.path.join(self.directory, file_name)
            xml_file_path = os.path.join(self.directory, file_name.replace('.gz', ''))

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

            downloaded_update_file_paths.append(xml_file_path)

        return downloaded_update_file_paths

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

get_doc = GetDoc(['30345029'], ['pubmed19n0975'], '/data/data_loading/pubmed_2019/pubmed2019_updates/source_files')
get_doc.run()