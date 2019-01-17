
from data_load.base.constants import DATA_LOADING_DIRECTORY, ID_PUBMED
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig
from data_load.base.utils.export_doc_ids import export_doc_ids

import psutil
import sys

from data_load.base.utils import file_utils


ID_PUBMED_2019 = 'PUBMED_2019'

SERVER = 'http://localhost:9200'
OLD_INDEX = 'pubmed2018_v5'
OLD_TYPE = 'article'

NEW_INDEX = 'pubmed2019'
NEW_TYPE = 'article'


missing_ids_directory = DATA_LOADING_DIRECTORY + '/' + ID_PUBMED_2019.lower() + '/' + 'missing_ids'


class FindMissingIds(object):

    def __init__(self):
        self.missing_ids = {}
        self.new_ids = {}

    def run(self):
        old_ids = export_doc_ids(server=SERVER,
                                src_index=OLD_INDEX,
                                src_type=OLD_TYPE)

        new_ids = export_doc_ids(server=SERVER,
                                src_index=NEW_INDEX,
                                src_type=NEW_TYPE)

        for _id in old_ids:
            if _id not in new_ids:
                self.missing_ids[_id] = 0
                if len(self.missing_ids) % 1000 == 0:
                    print 'Missing ids', len(self.missing_ids)

        for _id in new_ids:
            if _id not in old_ids:
                self.new_ids[_id] = 0
                if len(self.new_ids) % 1000 == 0:
                    print 'New ids', len(self.new_ids)

        print 'Missing ids', len(self.missing_ids)
        print 'New ids', len(self.new_ids)

        file_utils.make_directory(missing_ids_directory)

        file_utils.save_file(missing_ids_directory, 'missing_ids.json', self.missing_ids.keys())
        file_utils.save_file(missing_ids_directory, 'new_ids.json', self.new_ids)


find_missing_ids = FindMissingIds()
find_missing_ids.run()


