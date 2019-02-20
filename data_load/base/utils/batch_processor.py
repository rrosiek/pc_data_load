import os
import json
from multiprocessing import Process
import time

from data_load.base.utils import file_utils
from data_load.base.utils import export_doc_ids

BATCH_DOC_COUNT = 5000
TEMP_DIR = ''

PROCESSED_BATCHES_FILE = 'processed_batches.json'
ALL_IDS_FILE = 'all_ids.json'
RESULTS_FILE_PREFIX = 'results_'

class BatchProcessor(object):

    def __init__(self, load_config, batch_doc_count=BATCH_DOC_COUNT, multiprocess=False):
        self.load_config = load_config
        self.batch_doc_count = batch_doc_count
        self.multiprocess = multiprocess
        self.processes = []

    def run(self):
        self.process_batches()
        self.process_completed()

    def process_completed(self):
        pass

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

        print 'all_ids', len(all_ids)
        
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

    def get_processed_batches(self):
        batch_file_names = []
        for batch_file_name in os.listdir(self.batch_docs_directory()):
            file_path = os.path.join(self.batch_docs_directory(), batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith(RESULTS_FILE_PREFIX):
                batch_file_names.append(batch_file_name.replace(RESULTS_FILE_PREFIX, ''))

        return dict.fromkeys(batch_file_names)

    def process_batches(self):
        batch_file_names = []
        for batch_file_name in os.listdir(self.batch_docs_directory()):
            file_path = os.path.join(self.batch_docs_directory(), batch_file_name)
            if os.path.isfile(file_path) and batch_file_name.startswith('batch_'):
                batch_file_names.append(batch_file_name)

        if len(batch_file_names) == 0:
            batch_file_names = self.split_to_batches()
        
        batch_file_names.sort()
        print len(batch_file_names), 'Total batches'
        processed_batches = self.get_processed_batches()
        print len(processed_batches), 'Processed batches'

        filtered_batch_file_names = []
        for batch_file_name in batch_file_names:
            if batch_file_name not in processed_batches: 
                filtered_batch_file_names.append(batch_file_name)
        
        print len(filtered_batch_file_names), 'Batches remaining'

        raw_input('Continue?')
        for batch_file_name in filtered_batch_file_names:
            if self.multiprocess:
                process = Process(target=self.start_process_doc_batch, args=(batch_file_name,))
                process.start()

                self.processes.append(process)
                if len(self.processes) >= self.load_config.process_count:
                    old_process = self.processes.pop(0)
                    old_process.join()

                time.sleep(self.load_config.process_spawn_delay)
            else:
                self.start_process_doc_batch(batch_file_name)

    def start_process_doc_batch(self, batch_file_name):
        print 'Loading batch', batch_file_name
        batch = file_utils.load_file(self.batch_docs_directory(), batch_file_name)
        batch_name = batch_file_name.split('.')[0]
        results = self.process_docs_batch(batch, batch_name)
        file_utils.save_file(self.batch_docs_directory(), RESULTS_FILE_PREFIX + batch_file_name, results)

    def process_docs_batch(self, batch, batch_name):
        print 'Processing', batch_name, 'with', len(batch), 'docs'
        return {}


