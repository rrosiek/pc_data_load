
import os
import json
import time
import requests

from config import *
from data_load.base.constants import API_URL
import email_client
import pubmed_load_config
import data_load.base.utils.file_utils as file_utils


class FindProspectiveCitations(object):

    def __init__(self, logger, docs_with_new_citations):
        self.docs_with_new_citations = docs_with_new_citations
        self.logger = logger

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

    def get_prospective_docs_for_user(self, email, client_id):
        self.logger.info('Getting prospective docs for user: ' + str(email))

        query = {
            'email': email,
            'client_id': client_id
        }

        url = API_URL + 'prospective_docs_for_user/'

        prospective_docs_for_user = []

        response = requests.post(url, json=query)
        print response.text
        if response.status_code == 200 or response.status_code == 201:
            response_dict = json.loads(response.text)

            result = response_dict['result']
            prospective_docs_for_user = result['prospective_docs']

            # self.logger.info('Prospective docs for user: ' + str(email))
            # self.logger.info(str(prospective_docs_for_user))

            return prospective_docs_for_user
        else:
            print response.text

        return []

    def run(self):
        prospects = self.find_prospects()
        self.send_notifications(prospects)

        return prospects

    def send_notifications(self, prospects):
        # print 'Prospects', all_prospects
        self.logger.info('Prospects ' + str(prospects))
        failed_prospects = []

        # Send email notifications
        for prospect in prospects:
            problems = email_client.send_notification_for_prospect(prospect)
            if len(problems) > 0:
                failed_prospects.append({
                    'problems': problems,
                    'prospect': prospect
                })

        # Dump failed prospects to file
        if len(failed_prospects) > 0:
            file_utils.save_file(
                ROOT_DIRECTORY, 'failed_prospects.json', failed_prospects)

    def find_prospects(self):
        # Get the subscribed users
        subscribed_users = self.get_subscribed_users()
        self.logger.info('Subscribed users ' + str(subscribed_users))

        all_prospects = []

        # Get prospective cites for all users
        for user in subscribed_users:
            email = user['email']
            client_id = user['client_id']
            
            self.logger.info('-------------------------------------------------')
            self.logger.info('Finding prospective cites for user: ' + str(email))

            docs_with_matching_citations = []

            prospective_docs_for_user = self.get_prospective_docs_for_user(
                email, client_id)
            prospective_docs_for_user_str = {}
            for pmid in prospective_docs_for_user:
                prospective_docs_for_user_str[str(pmid)] = 0

            self.logger.info('Prospective docs for user: ' + str(len(prospective_docs_for_user_str)))
            self.logger.info('Docs with new citations: ' + str(len(self.docs_with_new_citations)))

            for pmid in self.docs_with_new_citations:
                new_citations_for_pmid = self.docs_with_new_citations[pmid]
                matching_cites_for_pmid = []

                for cite in new_citations_for_pmid:
                    if str(cite) in prospective_docs_for_user_str:
                        matching_cites_for_pmid.append(cite)

                if len(matching_cites_for_pmid) > 0:
                    docs_with_matching_citations.append({
                        '_id': pmid,
                        'matching_citations': matching_cites_for_pmid
                    })

            self.logger.info('Docs with matching citations for ' + email + ': ' + str(len(docs_with_matching_citations)))

            if len(docs_with_matching_citations) > 0:
                new_prospect = dict()
                new_prospect['email'] = email
                new_prospect['client_id'] = client_id
                new_prospect[
                    'docs_with_matching_citations'] = docs_with_matching_citations

                all_prospects.append(new_prospect)

        return all_prospects



# def get_prospective_docs_for_user( email, client_id):
#     print 'Getting prospective docs for user: ' , str(email)

#     query = {
#         'email': email,
#         'client_id': client_id
#     }

#     url = API_URL + 'prospective_docs_for_user/'

#     prospective_docs_for_user = []

#     response = requests.post(url, json=query)
#     print response.text
#     if response.status_code == 200 or response.status_code == 201:
#         response_dict = json.loads(response.text)

#         result = response_dict['result']
#         prospective_docs_for_user = result['prospective_docs']

#         print 'Prospective docs for user: ', str(email)
#         print str(prospective_docs_for_user)

#         return prospective_docs_for_user
#     else:
#         print response.text

#     return []

# get_prospective_docs_for_user('zohas@niaid.nih.gov', 1)
# get_prospective_docs_for_user('admin@altum.com', 1)
# get_prospective_docs_for_user('ken.fang@mobomo.com', 1)
