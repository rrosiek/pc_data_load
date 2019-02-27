
import os
import pubmed_load_config

from data_load.base.data_source_xml import XMLDataSource

class CountIds(object):

    def __init__(self):
        self.load_config = pubmed_load_config.get_load_config()
        self.unique_ids = {}

    def start(self):
        source_files_directory = self.load_config.source_files_directory()

        file_names = []

        for name in os.listdir(source_files_directory):
            file_path = os.path.join(source_files_directory, name)
            if os.path.isfile(file_path) and name.startswith('pubmed18n'):
                file_names.append(name)

        file_names.sort()

        for name in file_names:
            if name == 'pubmed18n0929.xml':
                break

            file_path = os.path.join(source_files_directory, name)
            
            print 'Processing file', file_path
            xml_data_source = XMLDataSource(file_path, 2)
            xml_data_source.process_rows(self.count_row)

        print len(self.unique_ids), 'unique ids'

    def count_row(self, doc, current_index):
        pmid = self.load_config.data_extractor.extract_id('', doc)
        if pmid is not None:
            self.unique_ids[pmid] = 0

        if current_index % 1000 == 0:
            print len(self.unique_ids)

        return True
  
def run():
    count_ids = CountIds()
    count_ids.start()

run()