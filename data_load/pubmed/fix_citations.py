
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper

from data_load.base.utils.batch_processor import BatchProcessor
from data_load.base.utils.data_utils  import DataUtils

from data_load.base.load_config import LoadConfig
from data_load.base.constants import ID_PUBMED

import psutil

class FixCitations(BatchProcessor):

    def __init__(self, load_config):
        super(FixCitations, self).__init__(load_config)
        self.load_config = load_config
        self.data_utils = DataUtils()
        self.citation_errors = {}

    def get_batch_docs_directory(self):
        return '/data/data_loading/pubmed_2019/pubmed2019/fix_citations'

    def get_query(self):
        return {
                    "exists": {
                        "field": "update_history"
                    }
                }

    def process_docs_batch(self, batch):
        print 'Fetching docs', len(batch)
        self.data_utils.batch_fetch_docs_for_ids(base_url=self.load_config.server,
                                                ids=batch,
                                                index=self.load_config.index,
                                                type=self.load_config.type,
                                                docs_fetched=self.docs_fetched,
                                                batch_size=500)
   
    def docs_fetched(self, docs, index, type):
        print 'Docs fetched', len(docs)
        docs_to_process = {}

        # print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                docs_to_process[_id] = existing_doc

        self.process_docs(docs_to_process)

    def process_docs(self, docs):
        print 'Processing docs', len(docs)
        for _id in docs:
            print 'Processing doc', _id
            doc = docs[_id]

            citations_from_update_history = []
            if 'update_history' in doc:
                update_history = doc['update_history']
                citations_from_update_history = self.get_citations_from_update_history(update_history)

            current_citations = self.get_citations(doc)

            if len(current_citations) != len(citations_from_update_history):
                self.citation_errors[_id] = 0

            print _id, 'current citations:', len(current_citations), 'citations from update history:', len(citations_from_update_history)

    def get_citations_from_update_history(self, update_history):
        citations = []
        for update_history_item in update_history:
            if 'original_citations' in update_history_item:
                original_citations = update_history_item['original_citations']

                citations_set = set(citations)
                original_citations_set = set(original_citations)
                citations = list(citations_set | original_citations_set)

            else:
                if 'added_citations' in update_history_item:
                    added_citations = update_history_item['added_citations']

                    citations_set = set(citations)
                    added_citations_set = set(added_citations)
                    citations = list(citations_set | added_citations_set)

                if 'removed_citations' in update_history_item:
                    removed_citations = update_history_item['removed_citations']

                    citations = list(set(citations) - set(removed_citations))
        
        return citations

    def get_citations(self, doc):
        citations = []
        if doc is not None and 'citations' in doc:
            citations_array = doc['citations']

            for citation_item in citations_array:
                source = citation_item['source']
                index_id = citation_item['index_id']
                if source == self.load_config.source and index_id == ID_PUBMED:
                    citations = citation_item['ids']
                    break

        return citations


load_config = LoadConfig()
load_config.root_directory = '/data/data_loading/pubmed_2019/pubmed2019/fix_citations'
load_config.process_count = psutil.cpu_count()

load_config.server = 'http://localhost:9200'
load_config.server_username = ''
load_config.server_password = ''
load_config.index =  "pubmed2019"
load_config.type = "article"

load_config.data_mapper =  PubmedDataMapper()
load_config.data_extractor = PubmedDataExtractor()
load_config.max_memory_percent = 75

fix_citations = FixCitations(load_config)
fix_citations.run()