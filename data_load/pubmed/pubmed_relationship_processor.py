from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.utils.data_loader_utils import DataLoaderUtils

from data_load.base.utils.data_utils import DataUtils

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
        self.data_loader_utils = DataLoaderUtils(self.load_config.server, self.load_config.index, self.load_config.type, self.load_config.server_username, self.load_config.server_password)
        self.load_relationships = True

        self.docs_with_new_citations = {}
        self.docs_citations_history = {}

        self.data_utils = DataUtils()

    def get_docs_with_new_citations(self):
        return self.docs_with_new_citations

    def get_citations_history(self):
        return self.docs_citations_history

    def update_citations_history(self, new_doc,  _id, new_citations, existing_citations):
        # Update citation history 
        if _id not in self.docs_citations_history:
            self.docs_citations_history[_id] = {}

        # Set the new doc flag
        self.docs_citations_history[_id]['new'] = new_doc

        # Update new citations
        if 'new_citations' not in self.docs_citations_history[_id]:
            self.docs_citations_history[_id]['new_citations'] = []

        self.docs_citations_history[_id]['new_citations'].extend(new_citations)

        # Update existing citations
        if 'existing_citations' not in self.docs_citations_history[_id]:
            self.docs_citations_history[_id]['existing_citations'] = []

        self.docs_citations_history[_id]['existing_citations'].extend(existing_citations)


    def process_relationships(self, extracted_ids):
        all_updated_ids = {}
        all_indexed_ids = {}

        if 'updated_ids' in self.data_source_summary:
            all_updated_ids = self.data_source_summary['updated_ids']

        if 'indexed_ids' in self.data_source_summary:
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

                self.update_citations_history(False, _id, new_citations, existing_citations)

                added_citations = []
                removed_citations = []
                citations_to_update = []

                # Get removed and existing citations
                for existing_citation in existing_citations:
                    if existing_citation in new_citations:
                        citations_to_update.append(existing_citation)
                    else:
                        removed_citations.append(existing_citation)

                        # Update cited_bys for the removed_citation doc
                        cited_bys_for_removed_citation = self.get_cited_bys_for_doc(existing_citation)
                        if _id in cited_bys_for_removed_citation:
                            cited_bys_for_removed_citation.remove(_id)

                        if existing_citation not in pubmed_cited_bys_pubmed:
                            pubmed_cited_bys_pubmed[existing_citation] = []

                        pubmed_cited_bys_pubmed[existing_citation].extend(cited_bys_for_removed_citation)


                # Get added citations
                for new_citation in new_citations:
                    if new_citation not in existing_citations:
                        added_citations.append(new_citation)
                        citations_to_update.append(new_citation)

                for citation in citations_to_update:
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

                if len(added_citations) > 0:
                    if _id not in self.docs_with_new_citations:
                        self.docs_with_new_citations[_id]= []
                    
                    self.docs_with_new_citations[_id].extend(added_citations)

                # Update existing doc with update file and update date
                # Update existing doc with added and removed citations
                # if len(removed_citations) > 0:
                self.update_doc(_id, existing_doc, existing_citations, removed_citations, added_citations)

            else:
                # New doc
                # Update citations history
                self.update_citations_history(True, _id, new_citations, [])

                # Get existing cited bys (citations from other existing docs) for the new doc
                existing_cited_bys = self.get_existing_cited_bys(_id)

                for cited_by in existing_cited_bys:
                    if _id not in pubmed_cited_bys_pubmed:
                        pubmed_cited_bys_pubmed[_id] = []
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

                if len(new_citations) > 0:
                    if _id not in self.docs_with_new_citations:
                        self.docs_with_new_citations[_id]= []
                        
                    self.docs_with_new_citations[_id].extend(new_citations)

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


    def get_cited_bys_for_doc(self, _id):
        doc = self.get_existing_doc(_id)
        return self.get_cited_bys(doc)

    # Fetch existing doc from elasticsearch
    def get_existing_doc(self, _id):
        exisiting_doc = self.data_loader_utils.fetch_doc(_id)
        if exisiting_doc is not None and '_source' in exisiting_doc:
            exisiting_doc = exisiting_doc['_source']
        return exisiting_doc

    def get_cited_bys(self, doc):
        cited_bys = []
        if doc is not None and 'cited_bys' in doc:
            cited_bys_array = doc['cited_bys']

            for cited_by_item in cited_bys_array:
                source = cited_by_item['source']
                index_id = cited_by_item['index_id']
                if source == self.load_config.source and index_id == ID_PUBMED:
                    cited_bys = cited_by_item['ids']
                    break

        return cited_bys

    # Get citations from doc
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

    def update_doc(self, _id, existing_doc, original_citations, removed_citations, added_citations):
        if len(removed_citations) > 0  or len(added_citations) > 0:
            print 'Updating doc:', _id, 'original_citations', len(original_citations), 'removed_citations', len(removed_citations), 'added_citations', len(added_citations)
        now = datetime.datetime.now()

        updated_date = now.isoformat()
        update_file = os.path.basename(self.data_source.data_source_file_path)

        # Create the update history item
        update_history_item = {
            "updated_date": updated_date,
            "update_file": update_file,
            "removed_citations": removed_citations,
            "added_citations": added_citations
        }

        # Get the existing update history
        update_history = []
        if 'update_history' in existing_doc:
            update_history = existing_doc['update_history']

        # Add the original citations list if not present
        if len(update_history) == 0:
            update_history.append({
                "original_citations": original_citations
            })
            
        # Add the new update history item
        update_history.append(update_history_item)

        doc = {
            "update_history": update_history
        }
        
        doc = {
            'doc': doc
        }

        self.data_loader_utils.update_doc(_id, doc)
