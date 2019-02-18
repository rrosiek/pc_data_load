from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.utils.data_loader_utils import DataLoaderUtils

from data_load.base.utils.data_utils import DataUtils
from data_load.base.utils.log_utils import LOG_LEVEL_TRACE, LOG_LEVEL_DEBUG

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

        self.existing_docs = {}

        self.data_utils = DataUtils()

    def docs_fetched(self, docs, index, type):
        self.load_config.log(LOG_LEVEL_TRACE, 'Docs fetched', len(docs))
        for doc in docs:
            _id = doc['_id']
            if '_source' in doc:
                existing_doc = doc['_source']
                self.existing_docs[_id] = existing_doc

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
        # all_indexed_ids = {}
        # if 'indexed_ids' in self.data_source_summary:
        #     all_indexed_ids = self.data_source_summary['indexed_ids']

        all_updated_ids = {}
        if 'updated_ids' in self.data_source_summary:
            all_updated_ids = self.data_source_summary['updated_ids']

        # Fetch existing (updated) docs
        self.load_config.log(LOG_LEVEL_DEBUG, 'Fetching docs', self.load_config.server, self.load_config.index, self.load_config.type)
    
        ids_to_fetch = all_updated_ids.keys()
        self.data_utils.batch_fetch_docs_for_ids(self.load_config.server,
                                                ids_to_fetch,
                                                self.load_config.index,
                                                self.load_config.type,
                                                self.docs_fetched,
                                                self.load_config.doc_fetch_batch_size,
                                                self.load_config.server_username,
                                                self.load_config.server_password)

        pubmed_citations_pubmed = {}
        pubmed_cited_bys_pubmed = {} 

        citations_to_remove = {}
        cited_bys_to_remove = {}
            
        count = 0
        for _id in extracted_ids:
            count += 1

            data = extracted_ids[_id]

            new_doc = False
            existing_citations = []
            new_citations = self.load_config.data_mapper.get_citations(data)
         
            if _id in all_updated_ids:
                # Existing doc
                existing_doc = self.get_existing_doc(_id)
                existing_citations = self.get_citations(existing_doc)
                new_doc = False
            else:
                new_doc = True

            self.update_citations_history(new_doc, _id, new_citations, existing_citations)
   
            added_citations = []
            removed_citations = []

            # Get removed citations
            for existing_citation in existing_citations:
                if existing_citation not in new_citations:
                    removed_citations.append(existing_citation)

            # Get added citations
            for new_citation in new_citations:
                if new_citation not in existing_citations:
                    added_citations.append(new_citation)

            # Added citations and cited bys
            for citation in added_citations:
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

            # Get existing cited bys (citations from other existing docs) for the new doc
            if new_doc:
                existing_cited_bys = self.get_existing_cited_bys(_id)

                for cited_by in existing_cited_bys:
                    if _id not in pubmed_cited_bys_pubmed:
                        pubmed_cited_bys_pubmed[_id] = []
                    if cited_by not in pubmed_cited_bys_pubmed[_id]:
                        pubmed_cited_bys_pubmed[_id].append(cited_by)

            # Removed citations and cited bys
            for removed_citation in removed_citations:
                # Removed citations
                if _id not in citations_to_remove:
                    citations_to_remove[_id] = []
                if removed_citation not in citations_to_remove[_id]:
                    citations_to_remove[_id].append(removed_citation)

                # Removed cited_bys
                if removed_citation not in cited_bys_to_remove:
                    cited_bys_to_remove[removed_citation] = []
                if _id not in cited_bys_to_remove[removed_citation]:
                    cited_bys_to_remove[removed_citation].append(_id)

            # Docs with new citations
            if len(added_citations) > 0:
                if _id not in self.docs_with_new_citations:
                    self.docs_with_new_citations[_id]= []
            self.docs_with_new_citations[_id].extend(added_citations)

            if count % 1000 == 0:
                print 'Processed', count, 'docs'

        pubmed_ids = {}
        pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                                        relations_array=pubmed_citations_pubmed,
                                                        dest_index_id=ID_PUBMED,
                                                        relationship_type=RELATIONSHIP_TYPE_CITATIONS,
                                                        removed_ids=citations_to_remove)

        pubmed_ids = self.load_config.data_mapper.reformat(reformatted_array=pubmed_ids,
                                                        relations_array=pubmed_cited_bys_pubmed,
                                                        dest_index_id=ID_PUBMED,
                                                        relationship_type=RELATIONSHIP_TYPE_CITED_BYS,
                                                        removed_ids=cited_bys_to_remove)

        print 'pubmed_cited_bys_pubmed', len(pubmed_cited_bys_pubmed)
        print 'pubmed_citations_pubmed', len(pubmed_citations_pubmed)
        print 'reformatted pubmed_ids', len(pubmed_ids)

        relationships = dict()
        relationships[ID_PUBMED] = pubmed_ids

        return relationships

    # def get_cited_bys_for_doc(self, _id):
    #     doc = self.fetch_existing_doc(_id)
    #     return self.get_cited_bys(doc)

    # Fetch existing doc from elasticsearch
    def fetch_existing_doc(self, _id):
        existing_doc = self.data_loader_utils.fetch_doc(_id)
        if existing_doc is not None and '_source' in existing_doc:
            existing_doc = existing_doc['_source']
        return existing_doc

    def get_existing_doc(self, _id):
        existing_doc = None
        if _id in self.existing_docs:
            existing_doc = self.existing_docs[_id]

        # Retry two times if not obtained in mget
        if existing_doc is None or len(existing_doc) == 0:
            existing_doc = self.fetch_existing_doc(_id)
            if existing_doc is None or len(existing_doc) == 0:
                existing_doc = self.fetch_existing_doc(_id)

        return existing_doc

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
