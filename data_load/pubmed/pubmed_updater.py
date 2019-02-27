from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_xml import XMLDataSource
from data_load.pubmed.pubmed_relationship_processor import PubmedRelationshipProcessor
from data_load.base.constants import API_URL, ID_PUBMED
from data_load.base.utils.export_doc_ids import get_doc_ids

import data_load.base.utils.file_utils as file_utils

import os
import json
import time
import requests

# from config import *

from email_client import EmailClient
from prospective_citations import FindProspectiveCitations

ALL_PUBMED_IDS_FILE = 'all_pubmed_ids.json'


DIR_DOCS_WITH_NEW_CITATION = 'docs_with_new_citations'
DIR_DOCS_CITATION_HISTORY = 'docs_citation_history'
DIR_PROSPECTS = 'prospects'
DIR_UPDATE_SUMMARY = 'update_summary'



class PubmedUpdater(object):

    def __init__(self, load_manager):
        self.load_manager = load_manager
        # self.file_summaries = {}
        self.existing_pubmed_ids = {}
        # self.docs_with_new_citations = {}

    def get_existing_pmids(self):
        load_config = self.load_manager.get_load_config()
        self.existing_pubmed_ids = get_doc_ids(server=load_config.server,
                                               src_index=load_config.index,
                                               src_type=load_config.type,
                                               dest_dir=load_config.other_files_directory(),
                                               dest_file_name=ALL_PUBMED_IDS_FILE)

        load_config.logger().info('Existing pmid count: ' + str(len(self.existing_pubmed_ids)))

    def clean_up_data_source_summary(self, data_source_summary):
        updated_ids = data_source_summary['updated_ids']
        indexed_ids = data_source_summary['indexed_ids']
        filtered_indexed_ids = {}

        for pmid in indexed_ids:
            if pmid in self.existing_pubmed_ids:
                updated_ids[pmid] = 0
            else:
                filtered_indexed_ids[pmid] = 0

        data_source_summary['updated_ids'] = updated_ids
        data_source_summary['indexed_ids'] = filtered_indexed_ids

        return data_source_summary

    def process_file(self, update_file):
        file_name = os.path.basename(update_file)

        load_config = self.load_manager.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]

        load_config.logger().info('Processing file: ' + str(file_name))
        load_config.process_count = 4

        data_processor = DataSourceProcessor(
            load_config, XMLDataSource(update_file, 2))
        data_processor.run()

        # Get data load summary
        data_source_summary = data_processor.get_combined_data_source_summary()
        
        # Clean up updated vs indexed ids
        data_source_summary = self.clean_up_data_source_summary(data_source_summary)

        # Save update summary
        self.save_update_summary(data_source_summary, update_file)

        return data_source_summary

        # Process relationships
        # self.process_relationships(update_file, data_source_summary)

    def process_relationships(self, update_file):
        file_name = os.path.basename(update_file)

        data_source_summary = self.load_update_summary(update_file)
      
        load_config = self.load_manager.get_load_config()
        load_config.data_source_name = file_name.split('.')[0] + '_relations'
        load_config.process_count = 4

        load_config.append_relations = True
        load_config.source = ''
        
        # print 'Processing relationships......'
        load_config.logger().info('Processing relationships: ' + str(file_name))

        data_processor = PubmedRelationshipProcessor(load_config, XMLDataSource(update_file, 2), data_source_summary)
        data_processor.run()
        
        docs_with_new_citations = data_processor.get_docs_with_new_citations()
        # Save docs with new citations
        self.save_docs_with_new_citations(docs_with_new_citations, update_file)

        # Save docs citation history
        docs_citations_history = data_processor.get_citations_history()
        self.save_docs_citations_history(docs_citations_history, update_file)

        return docs_with_new_citations

    def save_new_pmids(self, update_files):
        load_config = self.load_manager.get_load_config()
        self.existing_pubmed_ids = file_utils.load_file(load_config.other_files_directory(), ALL_PUBMED_IDS_FILE)

        update_summary = self.generate_update_summary(update_files)
        for update_file in update_summary:
            update_summary_for_file = update_summary[update_file]
            articles_processed = update_summary_for_file['articles_processed']
            for _id in articles_processed:
                self.existing_pubmed_ids[_id] = None

        file_utils.save_file(load_config.other_files_directory(), ALL_PUBMED_IDS_FILE, self.existing_pubmed_ids)

    def get_update_records_directory(self, sub_directory=None):
        load_config = self.load_manager.get_load_config()
        other_files_directory = load_config.other_files_directory()
        
        update_records_directory = other_files_directory + '/' + 'update_records'
        if sub_directory is not None:
            update_records_directory += '/' + sub_directory
        file_utils.make_directory(update_records_directory)
        return update_records_directory

    def save_update_summary(self, update_summary, update_file):
        update_record_file_name = self.get_update_summary_file_name(update_file)
        file_utils.save_file(self.get_update_records_directory(DIR_UPDATE_SUMMARY), update_record_file_name, update_summary)    

    def load_update_summary(self, update_file):
        update_record_file_name = self.get_update_summary_file_name(update_file)
        # print 'update_record_file_name', update_record_file_name
        return file_utils.load_file(self.get_update_records_directory(DIR_UPDATE_SUMMARY), update_record_file_name)

    def save_docs_with_new_citations(self, docs_with_new_citations, update_file):
        update_record_file_name = self.get_docs_with_new_citations_file_name(update_file)
        file_utils.save_file(self.get_update_records_directory(DIR_DOCS_WITH_NEW_CITATION), update_record_file_name, docs_with_new_citations)    

    def load_docs_with_new_citations(self, update_file):
        update_record_file_name = self.get_docs_with_new_citations_file_name(update_file)
        return file_utils.load_file(self.get_update_records_directory(DIR_DOCS_WITH_NEW_CITATION), update_record_file_name)    

    def save_docs_citations_history(self, doc_citations_history, update_file):
        update_record_file_name = self.get_docs_citations_history_file_name(update_file)
        file_utils.save_file(self.get_update_records_directory(DIR_DOCS_CITATION_HISTORY), update_record_file_name, doc_citations_history)    


    def get_update_file_name(self, update_file):
        file_name = os.path.basename(update_file)
        name = file_name.split('.')[0]
        return name

    def get_update_summary_file_name(self, update_file):
        name = self.get_update_file_name(update_file)
        file_name = 'update_summary_' + name + '.json'
        return file_name

    def get_docs_with_new_citations_file_name(self, update_file):
        name = self.get_update_file_name(update_file)
        file_name = 'docs_with_new_citations_' + name + '.json'
        return file_name

    def get_docs_citations_history_file_name(self, update_file):
        name = self.get_update_file_name(update_file)
        file_name = 'docs_citations_history_' + name + '.json'
        return file_name

    def get_new_pmids_per_file(self, update_files):
        new_pmids_per_file = {}

        for update_file in update_files:
            indexed_ids = {}
            data_source_summary = self.load_update_summary(update_file)
            batch_indexed_ids = data_source_summary['indexed_ids']
            for _id in batch_indexed_ids:
                if _id not in self.existing_pubmed_ids:
                    indexed_ids[_id] = 0

            new_pmids_per_file[update_file] = indexed_ids
            # print 'New pmids', len(indexed_ids)
        return new_pmids_per_file

    def get_total_updated_ids(self, update_files):
        total_updated_ids_per_file = {}

        for update_file in update_files:
            # indexed_ids = {}
            updated_ids = {}

            data_source_summary = self.load_update_summary(update_file)
            batch_indexed_ids = data_source_summary['indexed_ids']
            batch_updated_ids = data_source_summary['updated_ids']

            for _id in batch_indexed_ids:
                updated_ids[_id] = _id

            for _id in batch_updated_ids:
                updated_ids[_id] = _id

            total_updated_ids_per_file[update_file] = updated_ids

        # print  'Total updated pmids', total_updated_ids
        return total_updated_ids_per_file

    def generate_update_summary(self, update_files):
        # Update stats
        total_updated_ids_per_file = self.get_total_updated_ids(update_files)
        new_pmids_per_file = self.get_new_pmids_per_file(update_files)

        update_data = {}
        for update_file in update_files:
            total_updated_ids = {}
            new_pmids = {}

            if update_file in total_updated_ids_per_file:
                total_updated_ids = total_updated_ids_per_file[update_file]
            if update_file in new_pmids_per_file:
                new_pmids = new_pmids_per_file[update_file]

            update_data[update_file] = {
                'articles_processed': total_updated_ids,
                'new_articles': new_pmids
            }

        return update_data

    def get_docs_with_new_citations(self, update_files):
        # Merge docs with new citations from different update files
        docs_with_new_citations = {}
        for update_file in update_files:
            docs_with_new_citations_for_update_file = self.load_docs_with_new_citations(update_file)

            for pmid in docs_with_new_citations_for_update_file:
                if pmid not in docs_with_new_citations:
                    docs_with_new_citations[pmid] = []

                new_citations = docs_with_new_citations_for_update_file[pmid]
                docs_with_new_citations[pmid].extend(new_citations)
        
        return docs_with_new_citations


  
 