
import requests
import time
import threading
import urllib
from data_load.base.utils import file_utils

class CrossRefAPI(object):

    def __init__(self, session=None):
        self.base_url = 'https://api.crossref.org/'
        self.email = 'robint@qburst.com'

        # Request rate
        self.x_rate_limit_interval = 1 #secs
        self.x_rate_limit_limit = 50 #requests
        self.last_request_timestamp = None

        if session is None:
            session = requests.Session()
        self.session = session

        self.cursor = '*'

        self.request_queue = []
        self.result_items = []

        self.no_of_rows = 100

    def get_all_members(self):
        url = 'members'
        self.cursor = None
        self.create_request(url)
        print 'Total members', len(self.result_items)
        file_utils.save_file('/data_loading', 'crossref_members.json', self.result_items)
        return self.result_items

    def get_works_for_member_id(self, member_id):
        #https://api.crossref.org/members/5403/works
        self.cursor = '*'
        url = 'members/' + member_id + '/works'
        self.create_request(url)
        self.start_queue()
        return self.result_items

    def start_queue(self):
        threading.Timer(0, self.start_request_queue_processing).start()
        while len(self.request_queue) > 0:
            time.sleep(1)

    def create_request(self, url, data=None):
        request = {}
        print '-----------------------------------------------------------------------------------------------'
        print 'Creating request', url

        params  = {}
        params['mailto'] = self.email
        params['rows'] = self.no_of_rows
        if self.cursor is not None:
            params['cursor'] = self.cursor

        request['url'] = url
        request['params'] = params
        request['data'] = data

        self.queue_request(request)

    def queue_request(self, request):
        self.request_queue.append(request)

    def start_request_queue_processing(self):
        while len(self.request_queue) > 0:
            self.process_request_queue()
            time.sleep(1)

    def process_request_queue(self):
        time_since_last_request = self.time_since_last_request()
        required_delay = self.required_delay_between_requests()

        if time_since_last_request >= required_delay:
            if len(self.request_queue) > 0:
                request = self.request_queue[0]
                self.perform_request(request)
        else:
            delay = required_delay - time_since_last_request
            time.sleep(delay)

    def required_delay_between_requests(self):
        delay = float(self.x_rate_limit_interval) / self.x_rate_limit_limit
        print 'Required delay between requests:', delay, 'seconds'
        return delay

    def time_since_last_request(self):
        delay = 9999999999
        current_timestamp = time.time()
        if self.last_request_timestamp is not None:
            delay = current_timestamp - self.last_request_timestamp
            print 'Time since last request', delay, 'seconds'

        return delay

    def perform_request(self, request):
        url = request['url']
        params = request['params']
        data = request['data']

        params_string = urllib.urlencode(params)
        request_url = self.base_url + url + '?' + params_string

        print 'Perfoming request:', request_url
        response = self.session.get(request_url)
        if response.status_code == 200:
            self.request_queue.pop(0)
            self.process_response(url, data, response)

    def process_response(self, url, data, response):
        self.get_x_rate_limits_from_response(response)
        self.get_next_cursor_from_response(response)
        results = self.get_result_items_from_response(response)
        if len(results) == self.no_of_rows:
            self.create_request(url, data)
        self.result_items.extend(results)
        print 'Total results:', len(self.result_items)

    def get_x_rate_limits_from_response(self, response):
        headers = response.headers
        print headers
        if 'X-Rate-Limit-Interval' in headers:
           self.x_rate_limit_interval = headers['X-Rate-Limit-Interval']
           self.x_rate_limit_interval = self.x_rate_limit_interval.replace('s', '')
           self.x_rate_limit_interval = int(self.x_rate_limit_interval)

        if 'X-Rate-Limit-Limit' in headers:
            self.x_rate_limit_limit = int(headers['X-Rate-Limit-Limit'])

        print 'X-Rate-Limit-Limit:', self.x_rate_limit_limit
        print 'X-Rate-Limit-Interval:', self.x_rate_limit_interval

    def get_next_cursor_from_response(self, response):
        response_body = response.json()
        if 'message' in response_body:
            if 'next-cursor' in response_body['message']:
                self.cursor = response_body['message']['next-cursor']

        print 'Next cursor:', self.cursor

    def get_result_items_from_response(self, response):
        items = []
        response_body = response.json()
        if 'message' in response_body:
            if 'items' in response_body['message']:
                items = response_body['message']['items']
                print 'Result items:', len(items)

        return items
        

# crossref_api = CrossRefAPI()
# results = crossref_api.get_works_for_member_id('5403')
# # crossref_api.get_all_members()
# print '*****************************************************************************************Total results:', len(results)