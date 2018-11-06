from data_load.base.data_source_processor import DataSourceProcessor

from data_load.base.constants import ID_PUBMED, ID_CLINICAL_TRIALS
from data_load.base.constants import RELATIONSHIP_TYPE_CITATIONS
from data_load.base.constants import RELATIONSHIP_TYPE_CITED_BYS

class CTReferencesRelationshipProcessor(DataSourceProcessor):

    def __init__(self, load_config, data_source):
        super(CTReferencesRelationshipProcessor, self).__init__(load_config, data_source)
        self.load_relationships = True

    def process_relationships(self, extracted_ids):

        ct_citations_pubmed = {}
        pubmed_cited_bys_ct = {}

        count = 0
        for ct_id in extracted_ids:
            count += 1
            if count % 1000 == 0:
                print 'Processed relations', count, ct_id

            pmids = extracted_ids[ct_id]
            if ct_id not in ct_citations_pubmed:
                ct_citations_pubmed[ct_id] = []

            for pmid in pmids:
                ct_citations_pubmed[ct_id].append(pmid)

                if pmid not in pubmed_cited_bys_ct:
                    pubmed_cited_bys_ct[pmid] = []

                pubmed_cited_bys_ct[pmid].append(ct_id)

        ct_ids = {}
        ct_ids = self.load_config.data_mapper.reformat(reformatted_array=ct_ids,
                               relations_array=ct_citations_pubmed,
                               dest_index_id=ID_PUBMED,
                               relationship_type=RELATIONSHIP_TYPE_CITATIONS)

        pubmed_ids = {}
        pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                   relations_array=pubmed_cited_bys_ct,
                                   dest_index_id=ID_CLINICAL_TRIALS,
                                   relationship_type=RELATIONSHIP_TYPE_CITED_BYS)

        # count = 0
        # for _id in pubmed_ids:
        #     count += 1
        #     if count % 1000 == 0:
        #         print _id
        #
        print len(pubmed_ids), 'pubmed_ids'
        print len(ct_ids), 'ct_ids'

        # raw_input('Continue?')

        relationships = dict()
        relationships[ID_PUBMED] = pubmed_ids
        relationships[ID_CLINICAL_TRIALS] = ct_ids
        
        return relationships


# class CTRelationshipProcessor(RelationshipProcessor):

#     def __init__(self, load_config, data_source):
#         super(CTRelationshipProcessor, self).__init__(load_config, data_source)

#     def process_data_rows(self, data_source_batch_name):
#         # all_pubmed_ids = self.get_all_pubmed_ids()

#         ct_citations_pubmed = {}
#         pubmed_cited_bys_ct = {}

#         count = 0
#         for ct_id in self.data_source_batch:
#             count += 1
#             if count % 1000 == 0:
#                 print 'Processed relations', count, ct_id

#             pmids = self.data_source_batch[ct_id]
#             if ct_id not in ct_citations_pubmed:
#                 ct_citations_pubmed[ct_id] = []

#             for pmid in pmids:
#                 ct_citations_pubmed[ct_id].append(pmid)

#                 if pmid not in pubmed_cited_bys_ct:
#                     pubmed_cited_bys_ct[pmid] = []

#                 pubmed_cited_bys_ct[pmid].append(ct_id)

#         ct_ids = {}
#         ct_ids = self.reformat(reformatted_array=ct_ids,
#                                relations_array=ct_citations_pubmed,
#                                dest_index_id=ID_PUBMED,
#                                relationship_type=RELATIONSHIP_TYPE_CITATIONS)

#         pubmed_ids = {}
#         pubmed_ids = self.reformat(reformatted_array=pubmed_ids,
#                                    relations_array=pubmed_cited_bys_ct,
#                                    dest_index_id=ID_CLINICAL_TRIALS,
#                                    relationship_type=RELATIONSHIP_TYPE_CITED_BYS)

#         # count = 0
#         # for _id in pubmed_ids:
#         #     count += 1
#         #     if count % 1000 == 0:
#         #         print _id
#         #
#         print len(pubmed_ids), 'pubmed_ids'
#         print len(ct_ids), 'ct_ids'

#         # raw_input('Continue?')

#         self.process_relations_rows(ct_ids, data_source_batch_name, ID_CLINICAL_TRIALS)
#         self.process_relations_rows(pubmed_ids, data_source_batch_name, ID_PUBMED)

#     def get_all_pubmed_ids(self):
#         # TODO get all pubmed ids
#         return {}