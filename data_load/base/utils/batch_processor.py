import os
import json

from data_load.base.utils import file_utils
from data_load.base.utils import export_doc_ids

BATCH_DOC_COUNT = 5000
TEMP_DIR = ''

PROCESSED_BATCHES_FILE = 'processed_batches.json'
ALL_IDS_FILE = 'all_ids.json'

class BatchProcessor(object):

    def __init__(self, load_config, batch_doc_count=BATCH_DOC_COUNT):
        self.load_config = load_config
        self.batch_doc_count = batch_doc_count

    def run(self):
        self.process_batches()

    def get_query(self):
        query = {
            "match_all": {}
        }
        return query

    def get_batch_docs_directory(self):
        return TEMP_DIR

    def batch_docs_directory(self):
        directory = self.get_batch_docs_directory()
        file_utils.make_directory(directory)
        return directory

    def split_to_batches(self):
        print 'Fetching doc ids for', self.load_config.index, self.load_config.type
        query = self.get_query()
        print json.dumps(query)

        all_ids = export_doc_ids.get_doc_ids(self.load_config.server,
                                            self.load_config.index,
                                            self.load_config.type,
                                            self.batch_docs_directory(),
                                            ALL_IDS_FILE,
                                            query=query)
        all_ids = all_ids.keys()
        all_ids.sort()
        
        batch_file_names = []
        batch_index = 0
        batch_ids = []

        # Splitting into batches
        for _id in all_ids:
            batch_ids.append(_id)

            if len(batch_ids) >= self.batch_doc_count:
                print 'Writing batch:', batch_index
                batch_file_name = 'batch_' + str(batch_index) + '.json'
                batch_file_names.append(batch_file_name)
                file_utils.save_file(self.batch_docs_directory(), batch_file_name, batch_ids)
                batch_ids = []
                batch_index += 1

        if len(batch_ids) > 0:
            print 'Writing batch:', batch_index
            batch_file_name = 'batch_' + str(batch_index) + '.json'
            batch_file_names.append(batch_file_name)
            file_utils.save_file(self.batch_docs_directory(), batch_file_name, batch_ids)
            batch_index += 1

        return batch_file_names

    def process_batches(self):
        batch_file_names = []
        for batch_file_name in os.listdir(self.batch_docs_directory()):
            file_path = os.path.join(self.batch_docs_directory(), batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        if len(batch_file_names) == 0:
            batch_file_names = self.split_to_batches()
        
        batch_file_names.sort()
        print len(batch_file_names), 'total batches'
        processed_batches = file_utils.load_file(self.batch_docs_directory(), PROCESSED_BATCHES_FILE)
        print len(processed_batches), 'processed batches'

        raw_input('Continue?')
        for batch_file_name in batch_file_names:
            if batch_file_name not in processed_batches:
                print 'Loading batch', batch_file_name
                batch = file_utils.load_file(TEMP_DIR, batch_file_name)
                self.process_docs_batch(batch)
                processed_batches[batch_file_name] = 0
                file_utils.save_file(self.batch_docs_directory(), PROCESSED_BATCHES_FILE, processed_batches)

    def process_docs_batch(self, batch):
        print 'Processing batch with', len(batch), 'docs'
