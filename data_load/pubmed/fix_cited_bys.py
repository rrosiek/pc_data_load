
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper

from data_load.base.utils.batch_processor import BatchProcessor
from data_load.base.utils.data_utils  import DataUtils
from data_load.base.utils import file_utils

from data_load.base.load_config import LoadConfig
from data_load.base.constants import ID_PUBMED, RELATIONSHIP_TYPE_CITED_BYS
from data_load.base.relationship_loader import RelationshipLoader


import psutil


DIR = '/data/data_loading/pubmed_2019/pubmed2019/fix_cited_bys'

class FixCitations(BatchProcessor):

    def __init__(self, load_config):
        super(FixCitations, self).__init__(load_config, batch_doc_count=5000, multiprocess=False)
        self.load_config = load_config
        self.data_utils = DataUtils()

    def process_completed(self):
        pass

    def get_batch_docs_directory(self):
        return DIR

    # def get_query(self):
    #     return {
    #                 "exists": {
    #                     "field": "update_history"
    #                 }
    #             }

    def process_docs_batch(self, batch):
        print 'Processing docs', len(batch)
        pubmed_cited_bys_pubmed = {}
        for _id in batch:
            cited_bys = self.get_cited_bys(_id)
            pubmed_cited_bys_pubmed[_id] = cited_bys

            print _id, len(cited_bys)

        # pubmed_ids = {}
        # pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
        #                                                     relations_array=pubmed_cited_bys_pubmed,
        #                                                     dest_index_id=ID_PUBMED,
        #                                                     relationship_type=RELATIONSHIP_TYPE_CITED_BYS,
        #                                                     removed_ids=[])

        # relationship_loader = RelationshipLoader(self.load_config, pubmed_ids, self.load_config.index, self.load_config.type, 'ds_batch_fix_cited_bys')
        # relationship_loader.run()

    def get_cited_bys(self, _id):
        """
        Search elasticsearch for any docs citing the given id
        """

        url = self.load_config.server + '/' + self.load_config.index + '/' + self.load_config.type + '/_search'
        query = {
            "query": {
                "bool": {
                "must": [
                    {
                    "match": {
                        "citations.ids": _id
                    }
                    },
                    {
                    "match": {
                        "citations.source": ""
                    }
                    },
                    {
                    "match": {
                        "citations.index_id": ID_PUBMED
                    }
                    }
                ]
                }
            },
            "_source": [
                "_id"
            ]
        }

        response = self.data_utils.fetch_docs_for_query(url, query, self.load_config.server_username, self.load_config.server_password)
        if response is not None:
            hits = response['hits']
            hits = hits['hits']

            ids = []
            for doc in hits:
                _id = doc['_id']
                ids.append(_id)

            return ids

        return []   

load_config = LoadConfig()
load_config.root_directory = DIR
load_config.process_count = psutil.cpu_count()

load_config.server = 'http://localhost:9200'
load_config.server_username = ''
load_config.server_password = ''
load_config.index =  "pubmed2019"
load_config.type = "article"

load_config.data_mapper =  PubmedDataMapper()
load_config.data_extractor = PubmedDataExtractor()
load_config.max_memory_percent = 75

load_config.process_count = 8

load_config.source = ""
load_config.append_relations = False

fix_citations = FixCitations(load_config)
fix_citations.run()