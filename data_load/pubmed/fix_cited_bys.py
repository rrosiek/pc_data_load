
from data_load.pubmed2019.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2019.pubmed_data_mapper import PubmedDataMapper

from data_load.base.utils.batch_processor import BatchProcessor, PROCESSED_BATCHES_FILE, RESULTS_FILE_PREFIX
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

    def create_processed_files(self):
        processed_batches = file_utils.load_file(self.batch_docs_directory(), PROCESSED_BATCHES_FILE)
        for batch_file_name in processed_batches:
            file_utils.save_file(self.batch_docs_directory(), RESULTS_FILE_PREFIX + batch_file_name, {})

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

    def process_docs_batch(self, batch, batch_name):
        print 'Processing docs', len(batch)
        pubmed_cited_bys_pubmed = {}
        for _id in batch:
            cited_bys = self.get_cited_bys(_id)
            if len(cited_bys) > 0:
                pubmed_cited_bys_pubmed[_id] = cited_bys

            # print _id, len(cited_bys)

        pubmed_ids = {}
        pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                                            relations_array=pubmed_cited_bys_pubmed,
                                                            dest_index_id=ID_PUBMED,
                                                            relationship_type=RELATIONSHIP_TYPE_CITED_BYS,
                                                            removed_ids=[])

        
        print batch_name, len(pubmed_ids), 'ids to update'
        relationship_loader = RelationshipLoader(self.load_config, pubmed_ids, self.load_config.index, self.load_config.type, 'ds_batch_fix_cited_bys')
        relationship_loader.run()

        return {}

    def get_cited_bys(self, _id):
        """
        Search elasticsearch for any docs citing the given id
        """
        query = {
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
        }

        ids = self.data_utils.batch_fetch_ids_for_query(base_url=self.load_config.server, 
                                                query=query, 
                                                index=self.load_config.index, 
                                                type=self.load_config.type)

        return ids

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

load_config.process_count = 4
load_config.process_spawn_delay = 1

load_config.source = ""
load_config.append_relations = False

load_config.data_source_name = 'FixCitedBys'

fix_citations = FixCitations(load_config)
fix_citations.run()
# fix_citations.create_processed_files()