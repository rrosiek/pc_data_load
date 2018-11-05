from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.utils.data_loader_utils import DataLoaderUtils

import data_load.base.utils.data_utils as data_utils

from data_load.base.constants import ID_PUBMED
from data_load.base.constants import RELATIONSHIP_TYPE_CITATIONS
from data_load.base.constants import RELATIONSHIP_TYPE_CITED_BYS

import datetime
import os
import time

class PubmedRelationshipProcessor(DataSourceProcessor):
    def __init__(self, load_config, data_source, data_source_summary):
        super(PubmedRelationshipProcessor, self).__init__(load_config, data_source)
        self.data_source_summary = data_source_summary
        self.data_loader_utils = DataLoaderUtils(self.load_config.server, self.load_config.index, self.load_config.type)
        self.load_relationships = True

        self.docs_with_new_citations = {}

    def get_docs_with_new_citations(self):
        return self.docs_with_new_citations

    def process_relationships(self, extracted_ids):
        all_updated_ids = self.data_source_summary['updated_ids']
        all_indexed_ids = self.data_source_summary['indexed_ids']

        pubmed_citations_pubmed = {}
        pubmed_cited_bys_pubmed = {}

        count = 0

        for _id in extracted_ids:
            data = extracted_ids[_id]
            new_citations = self.load_config.data_mapper.get_citations(data)

            count += 1

            if _id in all_updated_ids:
                # Existing doc
                existing_doc = self.get_existing_doc(_id)

                # Retry two times to get the doc
                if existing_doc is None or len(existing_doc) == 0:
                    existing_doc = self.get_existing_doc(_id)
                    if existing_doc is None or len(existing_doc) == 0:
                        existing_doc = self.get_existing_doc(_id)

                # print 'Existing doc for id', _id
                # print existing_doc
                existing_citations = self.get_citations(existing_doc)

                added_citations = []
                removed_citations = []
                citations_to_update = []

                # Get removed and existing citations
                for exisiting_citation in existing_citations:
                    if exisiting_citation in new_citations:
                        citations_to_update.append(exisiting_citation)
                    else:
                        removed_citations.append(exisiting_citation)

                # Get added citations
                for new_citation in new_citations:
                    if new_citation not in existing_citations:
                        added_citations.append(new_citation)
                        citations_to_update.append(new_citation)

                for citation in citations_to_update:
                    # Citations
                    # if _id not in pubmed_citations_pubmed:
                    #     pubmed_citations_pubmed[_id] = []

                    # if citation not in pubmed_citations_pubmed[_id]:
                    #     pubmed_citations_pubmed[_id].append(citation)

                    # Cited by
                    if citation not in pubmed_cited_bys_pubmed:
                        pubmed_cited_bys_pubmed[citation] = []

                    if _id not in pubmed_cited_bys_pubmed[citation]:
                        pubmed_cited_bys_pubmed[citation].append(_id)

            
                # if len(citations_to_update) < len(existing_citations) and self.has_multiple_citations(existing_doc):
                #     print 'Existing doc', _id
                #     print 'Existing citations', len(existing_citations)
                #     print 'New citations', len(new_citations)
                #     print 'Updated citations', len(citations_to_update)
                #     print 'Doc', existing_doc
                #     print 'Data', data

                #     time.sleep(20)

                if len(added_citations) > 0:
                    self.docs_with_new_citations[_id] = added_citations

                # Update existing doc with update file and update date
                # Update existing doc with added and removed citations

                self.update_doc(_id, existing_doc, removed_citations, citations_to_update)

            elif _id in all_indexed_ids:
                # New doc

                # Get existing cited bys (citations from other existing docs) for the new doc
                existing_cited_bys = self.get_existing_cited_bys(_id)
                if _id not in pubmed_cited_bys_pubmed:
                    pubmed_cited_bys_pubmed[_id] = []

                for cited_by in existing_cited_bys:
                    if cited_by not in pubmed_cited_bys_pubmed[_id]:
                        pubmed_cited_bys_pubmed[_id].append(cited_by)

                # Process new citations
                for citation in new_citations:
                    # Citations
                    if _id not in pubmed_citations_pubmed:
                        pubmed_citations_pubmed[_id] = []

                    if citation not in pubmed_citations_pubmed[_id]:
                        pubmed_citations_pubmed[_id].append(citation)

                    # Cited by
                    if citation not in pubmed_cited_bys_pubmed:
                        pubmed_cited_bys_pubmed[citation] = []

                    if _id not in pubmed_cited_bys_pubmed[citation]:
                        pubmed_cited_bys_pubmed[citation].append(_id)

                self.docs_with_new_citations[_id] = new_citations

            if count % 1000 == 0:
                print 'Processed', count, 'docs'

        pubmed_ids = {}
        pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                                           relations_array=pubmed_citations_pubmed,
                                                           dest_index_id=ID_PUBMED,
                                                           relationship_type=RELATIONSHIP_TYPE_CITATIONS)

        pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                                           relations_array=pubmed_cited_bys_pubmed,
                                                           dest_index_id=ID_PUBMED,
                                                           relationship_type=RELATIONSHIP_TYPE_CITED_BYS)

        print 'pubmed_cited_bys_pubmed', len(pubmed_cited_bys_pubmed)
        print 'pubmed_citations_pubmed', len(pubmed_citations_pubmed)
        print 'reformatted pubmed_ids', len(pubmed_ids)

        relationships = dict()
        relationships[ID_PUBMED] = pubmed_ids

        return relationships

    # Fetch existing doc from elasticsearch
    def get_existing_doc(self, _id):
        exisiting_doc = self.data_loader_utils.fetch_doc(_id)
        if '_source' in exisiting_doc:
            exisiting_doc = exisiting_doc['_source']
        return exisiting_doc

    # Get citations from doc
    def get_citations(self, doc):
        citations = []
        if 'citations' in doc:
            citations_array = doc['citations']

            for citation_item in citations_array:
                source = citation_item['source']
                index_id = citation_item['index_id']
                if source == self.load_config.source and index_id == ID_PUBMED:
                    citations = citation_item['ids']
                    break

        return citations

    def has_multiple_citations(self, doc):
        citations = []
        if 'citations' in doc:
            citations_array = doc['citations']
            if len(citations_array) > 1:
                return True

        return False

    def get_existing_cited_bys(self, _id):
        """
        Search elasticsearch for any docs citing the given id
        """

        url = self.load_config.server + '/' + self.load_config.index + '/' + self.load_config.type + '/_search'
        query = {
            "query": {
                "match": {
                    "citations.ids": _id
                }
            },
            "_source": [
                "_id"
            ]
        }

        response = data_utils.fetch_docs_for_query(url, query)
        if response is not None:
            hits = response['hits']
            hits = hits['hits']

            ids = []
            for doc in hits:
                _id = doc['_id']
                ids.append(_id)

            return ids

        return []

    def update_doc(self, _id, existing_doc, removed_citations, citations_to_update):
        # print 'Updating doc', _id
        # return
        now = datetime.datetime.now()

        updated_date = now.isoformat()
        update_file = os.path.basename(self.data_source.data_source_file_path)

        if 'removed_citations' in existing_doc:
            existing_removed_citations = existing_doc['removed_citations']
            removed_citations.extend(existing_removed_citations)

        doc = {
            "updated_date": updated_date,
            "update_file": update_file,
            "removed_citations": removed_citations
        }

        existing_doc = self.load_config.data_mapper.update_citations_for_doc(_id,
                                                                             existing_doc,
                                                                             citations_to_update,
                                                                             '',
                                                                             ID_PUBMED,
                                                                             append=False)

        doc[RELATIONSHIP_TYPE_CITATIONS] = existing_doc[RELATIONSHIP_TYPE_CITATIONS]
        doc = {
            'doc': doc
        }

        self.data_loader_utils.update_doc(_id, doc)
