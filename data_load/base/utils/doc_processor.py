from data_load.base.utils.data_utils import DataUtils
from data_load.base.constants import ID_PUBMED, RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS, LOCAL_SERVER

class DocProcessor(object):

    def __init__(self, server, index, type):
        self.server = server
        self.src_index = src_index
        self.src_type = src_type

        self.docs_with_issues = {}
        self.processed_docs = 0

        self.data_utils = DataUtils()

    def run(self):
        self.processed_docs = 0
        query = {
            "match_all": {}
        }
        self.data_utils.batch_fetch_ids_for_query(base_url=self.server, index=self.src_index, type=self.src_type, query=query, ids_fetched=self.ids_fetched)

    def docs_fetched(self, docs, index, type):
        for doc in docs:
            self.process_doc(doc)

    def process_doc(self, doc):
        pass

    def ids_fetched(self, ids, index, type):
        self.data_utils.batch_fetch_docs_for_ids(base_url=self.server, ids=ids, index=self.src_index, type=self.src_type, docs_fetched=self.docs_fetched)



# def start():
#     doc_processor = DocProcessor(LOCAL_SERVER, 'pubmed2018_v5', 'article')
#     doc_processor.run()