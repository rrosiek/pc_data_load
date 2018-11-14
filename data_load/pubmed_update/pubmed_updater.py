from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_xml import XMLDataSource
from pubmed_relationship_processor import PubmedRelationshipProcessor

import data_load.base.utils.file_utils as file_utils

import os
import json
import time
import requests

from config import *
from data_load.base.constants import ID_PUBMED

import email_client
import pubmed_load_config

from data_load.base.utils.export_doc_ids import get_doc_ids

from prospective_citations import FindProspectiveCitations
from data_load.base.constants import API_URL

ALL_PUBMED_IDS_FILE = 'all_pubmed_ids.json'

class PubmedUpdater(object):

    def __init__(self, logger, update_files):
        self.logger = logger
        self.update_files = update_files
        self.file_summaries = {}

        self.session_key = None
        self.existing_pubmed_ids = {}

        self.docs_with_new_citations = {}

    def login(self):
        if self.session_key is None:
            data = {
                "email": "admin@altum.com",
                "password": "ocat"
            }

            url = API_URL + 'account/login/'

            response = requests.post(url, json=data)
            print response.status_code
            if response.status_code == 200 or response.status_code == 202:
                response_dict = json.loads(response.text)
                result = response_dict['result']
                self.session_key = result['session_key']
                print self.session_key
            else:
                print response.text
                return None

        return self.session_key

    def run(self):
        # Get existing pmids from file if exists
        load_config = pubmed_load_config.get_load_config()
        self.existing_pubmed_ids = get_doc_ids(server=load_config.server,
                                               src_index=load_config.index,
                                               src_type=load_config.type,
                                               dest_dir=load_config.other_files_directory(),
                                               dest_file_name=ALL_PUBMED_IDS_FILE)

        self.logger.info('Exisitng pmid count: ' + str(len(self.existing_pubmed_ids)))

        # Process update files
        for update_file in self.update_files:
            self.process_file(update_file)

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

        return data_source_summar

    def process_file(self, update_file):
        file_name = os.path.basename(update_file)

        load_config = pubmed_load_config.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
        load_config.set_logger(self.logger)

        self.logger.info('Processing file ' + str(update_file))
        self.logger.info('Processing docs... ' + str(file_name))

        data_processor = DataSourceProcessor(
            load_config, XMLDataSource(update_file, 2))
        data_processor.run()

        # Get data load summary
        data_source_summary = data_processor.get_combined_data_source_summary()
        # Clean up updated vs indexed ids
        data_source_summary = self.clean_up_data_source_summary(data_source_summary)

        self.file_summaries[update_file] = data_source_summary

        # Process relationships
        self.process_relationships(update_file, data_source_summary)


    def process_relationships(self, update_file, data_source_summary):
        file_name = os.path.basename(update_file)

        # print 'Processing relationships......'
        self.logger.info('Processing relationships... ' + str(file_name))

        load_config = pubmed_load_config.get_load_config()
        load_config.data_source_name = file_name.split('.')[0] + '_relations'
        load_config.process_count = 1
        load_config.set_logger(self.logger)

        load_config.append_relations = True
        load_config.source = ''

        data_processor = PubmedRelationshipProcessor(
            load_config, XMLDataSource(update_file, 2), data_source_summary)
        data_processor.run()
        
        docs_with_new_citations = data_processor.get_docs_with_new_citations()
        self.docs_with_new_citations[update_file] = docs_with_new_citations

    def get_docs_with_new_citations(self):
        # Merge docs with new citations from different update files
        docs_with_new_citations = {}
        for update_file in self.docs_with_new_citations:
            docs_with_new_citations_for_update_file = self.docs_with_new_citations[update_file]

            for pmid in docs_with_new_citations_for_update_file:
                if pmid not in docs_with_new_citations:
                    docs_with_new_citations[pmid] = []

                new_citations = docs_with_new_citations_for_update_file[pmid]
                docs_with_new_citations[pmid].extend(new_citations)
        
        return docs_with_new_citations

    # def send_notifications(self):
    #     new_pmids = self.get_new_pmids()
    #     subscribed_users = self.get_subscribed_users()
    #     all_prospects = []
    #     failed_prospects = []

    #     test_data_directory = 'test_data'
    #     file_utils.make_directory('test_data')

    #     file_utils.save_file(test_data_directory, 'new_pmids.json', new_pmids)

    #     self.logger.info('New pmids ' + str(len(new_pmids)))
    #     self.logger.info('Subscribed users ' + str(subscribed_users))

    #     # print 'New pmids', len(new_pmids)
    #     # print 'Subscribed users', subscribed_users

    #     # Get prospective cites for all users
    #     for user in subscribed_users:
    #         email = user['email']
    #         client_id = user['client_id']

    #         prospective_cites = self.get_prospective_cites(
    #             email, client_id, new_pmids)
    #         if prospective_cites is not None:
    #             all_prospects.extend(prospective_cites)

    #     # print 'Prospects', all_prospects
    #     self.logger.info('Prospects ' + str(all_prospects))

    #     # Send email notifications
    #     for prospect in all_prospects:
    #         problems = email_client.send_notification_for_prospect(prospect)
    #         if len(problems) > 0:
    #             failed_prospects.append({
    #                 'problems': problems,
    #                 'prospect': prospect
    #             })

    #     # Dump failed prospects to file
    #     if len(failed_prospects) > 0:
    #         file_utils.save_file(
    #             ROOT_DIRECTORY, 'failed_prospects.json', failed_prospects)

    #     return all_prospects

    def save_new_pmids(self):
        for update_file in self.file_summaries:
            data_source_summary = self.file_summaries[update_file]
            batch_indexed_ids = data_source_summary['indexed_ids']
            for _id in batch_indexed_ids:
                self.existing_pubmed_ids[_id] = None

        load_config = pubmed_load_config.get_load_config()
        file_utils.save_file(load_config.other_files_directory(), ALL_PUBMED_IDS_FILE, self.existing_pubmed_ids)

    def get_new_pmids(self):
        new_pmids = {}

        for update_file in self.file_summaries:
            indexed_ids = {}
            data_source_summary = self.file_summaries[update_file]
            batch_indexed_ids = data_source_summary['indexed_ids']
            for _id in batch_indexed_ids:
                if _id not in self.existing_pubmed_ids:
                    new_pmids[_id] = 0
            # print 'New pmids', len(indexed_ids)
        return new_pmids.keys()

    # def get_new_pmids(self):
    #     new_pmids = []

    #     for update_file in self.docs_with_new_citations:
    #         new_pmids.extend(self.docs_with_new_citations[update_file])

    #     return new_pmids

    # def get_new_pmids(self):
    #     test_data_directory = 'test_data'
    #     file_utils.make_directory('test_data')

    #     new_pmids = file_utils.load_file(test_data_directory, 'new_pmids.json')

    #     return new_pmids

    def get_new_pmids_per_file(self):
        new_pmids_per_file = {}

        for update_file in self.file_summaries:
            indexed_ids = {}
            data_source_summary = self.file_summaries[update_file]
            batch_indexed_ids = data_source_summary['indexed_ids']
            for _id in batch_indexed_ids:
                if _id not in self.existing_pubmed_ids:
                    indexed_ids[_id] = 0

            new_pmids_per_file[update_file] = indexed_ids
            # print 'New pmids', len(indexed_ids)
        return new_pmids_per_file

    def get_total_updated_ids(self):
        total_updated_ids_per_file = {}

        for update_file in self.file_summaries:
            # indexed_ids = {}
            updated_ids = {}

            data_source_summary = self.file_summaries[update_file]
            batch_indexed_ids = data_source_summary['indexed_ids']
            batch_updated_ids = data_source_summary['updated_ids']

            for _id in batch_indexed_ids:
                updated_ids[_id] = _id

            for _id in batch_updated_ids:
                updated_ids[_id] = _id

            total_updated_ids_per_file[update_file] = updated_ids

        # print  'Total updated pmids', total_updated_ids
        return total_updated_ids_per_file

    def get_subscribed_users(self):
        url = API_URL + 'subscribed_users'

        subscribed_users = []

        response = requests.get(url)
        print response.text
        if response.status_code == 200 or response.status_code == 201:
            response_dict = json.loads(response.text)

            result = response_dict['result']
            for result_item in result:
                client_id = result_item['client_id']
                emails = result_item['subscribed_users']
                for email in emails:
                    subscribed_users.append({
                        'client_id': client_id,
                        'email': email
                    })

            return subscribed_users
        else:
            print response.text

        return []

    def get_prospective_cites(self, email, client_id, new_pmids):
        self.logger.info('Getting prospective cites for ' + email + ' New pmids: ' + str(len(new_pmids)))

        session_key = self.login()

        if session_key is not None:
            headers = {
                'Authorization': 'Bearer ' + session_key
            }

            query = {
                'email': email,
                'client_id': client_id,
                'indexes': [
                    {
                        'index_id': ID_PUBMED,
                        'index': INDEX,
                        'type': TYPE,
                        'ids': new_pmids
                    }
                ]
            }

            url = API_URL + 'prospective_cites/'
            # print query, url

            response = requests.post(url, json=query, headers=headers)
            if response.status_code == 200 or response.status_code == 201:
                response_dict = json.loads(response.text)

                result = response_dict['result']
                operation_id = result['operation_id']

                # get prospective cites results using operation id
                print 'Getting prospective cites success'
                print response.status_code
                print response.text
                return self.get_prospective_cites_result(email, client_id, operation_id)
            else:
                self.logger.info('Getting prospective cites failed for ' + email + ' ' + response.text)
        else:
            self.logger.info('Login failed')

        return None

    def get_prospective_cites_result(self, email, client_id, operation_id):
        self.logger.info('Getting prospective cites results for ' + email + ' ' + str(operation_id))
        prospective_cites = []

        session_key = self.login()

        if session_key is not None:
            headers = {
                'Authorization': 'Bearer ' + session_key
            }

            query = {
                'operation_id': operation_id
            }

            url = API_URL + 'operation/details/'

            operation_status = OPERATION_STATUS_RUNNING
            count = 0
            max_tries = 600

            # Retry until status changes to failed or success; or try count
            # reaches max_tries
            while operation_status == OPERATION_STATUS_RUNNING and count < max_tries:
                self.logger.info('Getting operation result, retry count ' + str(count))
                count += 1

                response = requests.post(url, json=query, headers=headers)
                if response.status_code == 200 or response.status_code == 201:
                    response_dict = json.loads(response.text)
                    result = response_dict['result']

                    if 'status' in result:
                        operation_status = int(result['status'])

                        if operation_status == OPERATION_STATUS_RUNNING:
                            self.logger.info('Operation ' + str(operation_id) + ' running...')
                        elif operation_status == OPERATION_STATUS_SUCCESS:
                            self.logger.info('Operation ' + str(operation_id) + ' success')
                            results = result['results']

                            new_prospect = dict()
                            new_prospect['email'] = email
                            new_prospect['client_id'] = client_id
                            new_prospect['matching_citations'] = []

                            for result_item in results:
                                # check if any prospective cites were found
                                if result_item['status'].lower() == 'Found prospective cites'.lower():
                                    new_prospect['matching_citations'].append(
                                        result_item)

                            if len(new_prospect['matching_citations']) > 0:
                                prospective_cites.append(new_prospect)

                        elif operation_status == OPERATION_STATUS_FAILED:
                            self.logger.info('Operation ' + str(operation_id) + ' failed')
                else:
                    self.logger.info('Getting prospective cites result failed for ' + str(email) + ' ' + response.text)

                # Wait 10 seconds before next retry
                time.sleep(10)
        else:
            self.logger.info('Login failed')

        self.logger.info(str(len(prospective_cites)) + ' prospective cites found for ' + email)

        return prospective_cites

 