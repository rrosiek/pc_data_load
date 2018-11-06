from data_load.base.utils.process_index import ProcessIndex
from data_load.base.data_mapper import DataMapper
from data_load.base.constants import ID_PUBMED, ID_CLINICAL_TRIALS, RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS
import data_load.base.utils.export_doc_ids as export_doc_ids
from data_load.pubmed2018.pubmed_data_extractor import PubmedDataExtractor
from data_load.pubmed2018.pubmed_data_mapper import PubmedDataMapper
from data_load.base.load_config import LoadConfig

from data_load.base.relationship_loader import RelationshipLoader
from data_load.base.data_load_batcher import DataLoadBatcher

import data_load.base.utils.file_utils as file_utils
import data_load.base.utils.data_utils as data_utils
import data_load.base.utils.es_utils as es_utils

import time

data_mapper = DataMapper()

class CTPublicationsRelationshipProcessor(object):

    def __init__(self, ct_load_config):
        self.ct_load_config = ct_load_config
        self.pubmed_load_config = self.get_pubmed_load_config()

        self.pubmed_relations = {}
        self.ct_relations = {}

        self.processed_docs = 0

    def run(self):
        # doc_ids = export_doc_ids( self.server, self.index,
        #                             self.type, self.index + '_' + self.type , 'doc_ids.json')
        doc_ids = file_utils.load_file( self.ct_load_config.index, self.ct_load_config.index + '_ids.json')

        if len(doc_ids) == 0:
            doc_ids = export_doc_ids.export_doc_ids(self.ct_load_config.server, self.ct_load_config.index, self.ct_load_config.type)

        doc_ids = doc_ids.keys()

        data_utils.batch_fetch_docs_for_ids(base_url= self.ct_load_config.server,
                                            ids=doc_ids,
                                            index= self.ct_load_config.index,
                                            type= self.ct_load_config.type,
                                            docs_fetched=self.docs_fetched)

        print 'Total pubmed relations', len(self.pubmed_relations)
        print 'Total ct relations', len(self.pubmed_relations)

        # Load Pubmed relations
        pubmed_ids = {}
        pubmed_ids = data_mapper.reformat(reformatted_array=pubmed_ids,
                                          relations_array=self.pubmed_relations,
                                          dest_index_id=ID_CLINICAL_TRIALS,
                                          relationship_type=RELATIONSHIP_TYPE_CITATIONS)

        print 'Reformatted pubmed ids', len(pubmed_ids)

        self.pubmed_load_config.append_relations = True
        self.pubmed_load_config.source = 'ct_publications'
        self.pubmed_load_config.data_source_name = 'ct_publications_relations'

        data_load_batcher = DataLoadBatcher(self.pubmed_load_config,  self.pubmed_load_config.index,  self.pubmed_load_config.type)
        data_load_batcher.load_relationships = True
        data_load_batcher.process_data_rows('pubmed_ct_citations', pubmed_ids)

        # Load Clinical trials relations
        ct_ids = {}
        ct_ids = data_mapper.reformat(reformatted_array=ct_ids,
                                      relations_array=self.ct_relations,
                                      dest_index_id=ID_PUBMED,
                                      relationship_type=RELATIONSHIP_TYPE_CITED_BYS)
        print 'Reformatted ct ids', len(ct_ids)

        self.ct_load_config.append_relations = True
        self.ct_load_config.source = 'ct_publications'
        self.ct_load_config.data_source_name = 'ct_publications_relations'

        data_load_batcher = DataLoadBatcher(self.ct_load_config,  self.ct_load_config.index,  self.ct_load_config.type)
        data_load_batcher.load_relationships = True
        data_load_batcher.process_data_rows('ct_pubmed_cited_bys', ct_ids)

    def get_pubmed_load_config(self):
        index_item = es_utils.get_info_for_index_id(ID_PUBMED)
        pubmed_index = index_item['index']
        pubmed_type = index_item['index_type']

        load_config = LoadConfig()
        load_config.root_directory = self.ct_load_config.root_directory

        load_config.server = self.ct_load_config.server
        load_config.index = pubmed_index
        load_config.type = pubmed_type

        load_config.data_extractor = PubmedDataExtractor()
        load_config.data_mapper = PubmedDataMapper()

        return load_config

    def docs_fetched(self, docs, index, type):
        docs_to_process = {}

        print 'Docs fetched', len(docs)
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                docs_to_process[_id] = existing_doc

        self.process_docs(docs_to_process) 

        self.processed_docs += len(docs)
        print 'Processed docs', self.processed_docs, 'Pubmed relations', len(self.pubmed_relations) 

    def process_docs(self, docs):
        bulk_data = ''

        for _id in docs:
            doc = docs[_id]
            processed_doc = self.process_doc(_id, doc)
            
    def process_doc(self, _id, doc):
        if 'ct_publications' in doc:
            cited_bys = []
            ct_publications = doc['ct_publications']

            for ct_publication in ct_publications:
                if 'pmid' in ct_publication:
                    pmid = ct_publication['pmid']
                    pmid = str(pmid) 
                    if len(pmid) > 0:
                        cited_bys.append(pmid)

                        if pmid not in self.pubmed_relations:
                            self.pubmed_relations[pmid] = []

                        self.pubmed_relations[pmid].append(_id)

                        if _id not in self.ct_relations:
                            self.ct_relations[_id] = []

                        self.ct_relations[_id].append(pmid)
            
        return None


# class ProcessCT(object):

#     def process_doc(self, _id, doc):
#         if 'ct_publications' in doc:
#             cited_bys = []
#             ct_publications = doc['ct_publications']

#             for ct_publication in ct_publications:
#                 if 'pmid' in ct_publication:
#                     pmid = ct_publication['pmid']
#                     pmid = str(pmid) 
#                     if len(pmid) > 0:
#                         cited_bys.append(pmid)

#                         # if pmid not in pubmed_relations:
#                         #     pubmed_relations[pmid] = []

#                         # pubmed_relations[pmid].append(_id)

#                         # print len(pubmed_relations)

#             if len(cited_bys) > 0:
#                 # print 'Existing doc: ', doc
#                 doc = data_mapper.update_cited_bys_for_doc(_id, doc, cited_bys, 'pardi', ID_PUBMED, append=True)
#                 # print 'Updated doc: ', doc
#                 # time.sleep(20)
#                 return doc
            
#         return None

#     def run(self):
#         process_index = ProcessIndex(src_server, src_index, src_type, self.process_doc)

#         process_index.batch_size = 2500
#         process_index.process_count = 16
#         process_index.process_spawn_delay = 0.15
#         process_index.bulk_data_size = 300000
        
#         process_index.run()

#         # print len(pubmed_relations), 'pubmed_relations'



# def run():
#     process_ct = ProcessCT()
#     process_ct.run()

#     # find_pubmed_ct_relations = FindPubmedCTRelations(src_server, src_index, src_type)
#     # find_pubmed_ct_relations.run()

# run()
