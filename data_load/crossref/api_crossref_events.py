
import requests
import time
import json
import threading
import urllib
from data_load.base.utils import file_utils
from data_load.crossref.api_base import APIBase

from data_load.crossref.api_crossref import CrossRefAPI

class CrossRefEventsAPI(APIBase):

    def __init__(self, session=None):
        super(CrossRefEventsAPI, self).__init__(session)

        self.base_url = 'https://api.eventdata.crossref.org/v1/'
        self.email = 'robint@qburst.com'
        
        self.cursor = None
        self.no_of_rows = 1000

    def get_events_for_doi_prefix(self, doi_prefix):
        url = 'events'
        params = {}
        params['obj-id.prefix'] = doi_prefix

        events = self.get_events(url, params)

        print len(events), 'total events for doi prefix:', doi_prefix
        return events

    def stream_events_for_doi_prefix(self, doi_prefix, events_fetched, cursor=None):
        self.cursor = None
        if cursor is not None: 
            self.cursor = cursor
            
        url = 'events'
        params = {}
        params['obj-id.prefix'] = doi_prefix

        total_results = 0

        request = self.create_request(url, params)
        response = self.perform_request(request)
        results = self.get_events_from_response(response)
        results = self.add_doi_prefix_to_results(results, doi_prefix)
        total_results += len(results)
        if events_fetched is not None:
            events_fetched(self.cursor, results)
        print 'Total results:', total_results
        while self.cursor is not None and len(results) == self.no_of_rows:
            request = self.create_request(url, params)
            response = self.perform_request(request)
            results = self.get_events_from_response(response)
            results = self.add_doi_prefix_to_results(results, doi_prefix)
            total_results += len(results)
            if events_fetched is not None:
                events_fetched(self.cursor, results)
            print 'Total results:', total_results

        print 'Streaming events end'
        return total_results

    def get_events(self, url, params):
        result_items = []

        request = self.create_request(url, params)
        response = self.perform_request(request)
        results = self.get_events_from_response(response)
        result_items.extend(results)
        print len(result_items), 'results for cursor', self.cursor
        while self.cursor is not None and len(results) == self.no_of_rows:
            request = self.create_request(url, params)
            response = self.perform_request(request)
            results = self.get_events_from_response(response)
            result_items.extend(results)
            print len(result_items), 'results for cursor', self.cursor

        return result_items

    def get_events_from_response(self, response):
        events = []
        if 'message' in response:
            if 'events' in response['message']:
                events = response['message']['events']
                # print 'Events:', len(events)

        return events
    
    def add_doi_prefix_to_results(self, results, doi_prefix):
        for result in results:
            result['doi_prefix'] = doi_prefix

            if 'obj_id' in result:
                obj_id = result['obj_id']

                if 'https://doi.org/' in obj_id:
                    doi = obj_id.replace('https://doi.org/', '')
                    result['doi'] = doi
                else:
                    print 'DOI url error:', obj_id

        return results

# crossref_api = CrossRefAPI()
# results = crossref_api.get_works_for_member_id('5403')
# # crossref_api.get_all_members()
# print '*****************************************************************************************Total results:', len(results)

# eventdata_api = CrossRefEventsAPI()
# crossref_api = CrossRefAPI()
# print json.dumps(crossref_api.search_member('mac'))

# print json.dumps(crossref_api.search_member('scienceopen'))

# print json.dumps(crossref_api.search_member('nature'))

# print json.dumps(crossref_api.get_data_for_member_id(5403))

# print json.dumps(crossref_api.get_data_for_member_id(297))

# doi_prefixes = crossref_api.get_doi_prefixes_for_member_ids([5403, 297])
# print len(doi_prefixes)

# for doi_prefix in doi_prefixes:
#     events = eventdata_api.get_events_for_doi_prefix(doi_prefix)

# print len(events), 'done'