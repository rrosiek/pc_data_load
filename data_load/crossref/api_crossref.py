
import requests
import time
import json
import threading
import urllib
from data_load.base.utils import file_utils
from data_load.crossref.api_base import APIBase

class CrossRefAPI(APIBase):

    def __init__(self, session=None):
        super(CrossRefAPI, self).__init__(session)

        self.base_url = 'https://api.crossref.org/'
        self.email = 'robint@qburst.com'

    def search_member(self, query):
        params = {}
        params['query'] = query

        url = 'members'
        self.cursor = None
        request = self.create_request(url=url, params=params)
        response = self.perform_request(request)
        result = self.get_message_from_response(response)

        return result

    def get_data_for_member_id(self, member_id):
        url = 'members/' + str(member_id)
        self.cursor = None
        request = self.create_request(url)
        response = self.perform_request(request)
        result = self.get_message_from_response(response)

        return result

    def get_doi_prefixes_for_member_ids(self, member_ids):
        prefixes_for_member_ids = []

        for member_id in member_ids:
            data_for_member_id = self.get_data_for_member_id(member_id)
            if 'prefixes' in data_for_member_id:
                prefixes = data_for_member_id['prefixes']
                print 'Prefixes for', member_id, json.dumps(prefixes)
                prefixes_for_member_ids.extend(prefixes)

        return prefixes_for_member_ids

    def get_all_members(self):
        result_items = []

        params = {}
        params['rows'] = self.no_of_rows
        url = 'members'
        self.cursor = None
        request = self.create_request(url, params)
        response = self.perform_request(request)
        result_items = self.get_result_items_from_response(response)

        print 'Total members', len(result_items)
        file_utils.save_file('/data_loading', 'crossref_members.json', result_items)
        return result_items

    def get_works_for_member_id(self, member_id):
        #https://api.crossref.org/members/5403/works

        self.cursor = '*'
        url = 'members/' + member_id + '/works'

        params = {}
        params['rows'] = self.no_of_rows

        # self.start_queue()
        return self.get_data_for_url(url, params)

    def stream_works_for_member_id(self, member_id, works_fetched, cursor=None):
        self.cursor = '*'
        if cursor is not None: 
            self.cursor = cursor
            
        url = 'members/' + member_id + '/works'

        params = {}
        params['rows'] = self.no_of_rows

        total_results = 0

        request = self.create_request(url, params)
        response = self.perform_request(request)
        results = self.get_result_items_from_response(response)
        total_results += len(results)
        if works_fetched is not None:
            works_fetched(self.cursor, results)
        print 'Total results:', total_results
        while self.cursor is not None and len(results) == self.no_of_rows:
            request = self.create_request(url, params)
            response = self.perform_request(request)
            results = self.get_result_items_from_response(response)
            total_results += len(results)
            if works_fetched is not None:
                works_fetched(self.cursor, results)
            print 'Total results:', total_results

        return total_results

    def get_data_for_url(self, url, params):
        result_items = []

        request = self.create_request(url, params)
        response = self.perform_request(request)
        results = self.get_result_items_from_response(response)
        result_items.extend(results)
        print 'Total results:', len(result_items)
        while self.cursor is not None and len(results) == self.no_of_rows:
            request = self.create_request(url, params)
            response = self.perform_request(request)
            results = self.get_result_items_from_response(response)
            result_items.extend(results)
            print 'Total results:', len(result_items)

        return result_items

    def get_result_items_from_response(self, response):
        items = []
        if 'message' in response:
            if 'items' in response['message']:
                items = response['message']['items']
                print 'Result items:', len(items)

        return items


# crossref_api = CrossRefAPI()
# results = crossref_api.get_works_for_member_id('5403')
# # crossref_api.get_all_members()
# print '*****************************************************************************************Total results:', len(results)

# crossref_api = CrossRefAPI()
# print json.dumps(crossref_api.search_member('mac'))

# print json.dumps(crossref_api.search_member('scienceopen'))

# print json.dumps(crossref_api.search_member('nature'))

# print json.dumps(crossref_api.get_data_for_member_id(5403))

# print json.dumps(crossref_api.get_data_for_member_id(297))

# print json.dumps(crossref_api.get_doi_prefixes_for_member_ids([5403, 297]))