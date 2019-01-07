import json
import time
import requests
from pprint import pprint


class DataLoaderUtils(object):

    def __init__(self, server, index, type, username='', password=''):
        self.server = server
        self.index = index
        self.type = type
        self.session = requests.Session()
        self.auth = (username, password)

    def set_server(self, server):
        self.server = server

    def bulk_index_header(self, _id):
        return '{ "index" : { "_index" : "' + self.index + '", "_type" : "' + self.type + '", "_id" : "' + _id + '" } }'

    def bulk_update_header(self, _id):
        return '{ "update" : { "_index" : "' + self.index + '", "_type" : "' + self.type + '", "_id" : "' + _id + '" } }'

    def bulk_delete_header(self, _id):
        return '{ "delete" : { "_index" : "' + self.index + '", "_type" : "' + self.type + '", "_id" : "' + _id + '" } }'

    def load_bulk_data(self, bulk_data):
        url = self.server + '/' + '_bulk'
        headers = {"content-type": "application/json"}
        response = self.session.post(url, data=bulk_data, headers=headers, auth=self.auth)
        if response.status_code == 200 or response.status_code == 201:
            return response.text
        else:
            print response.status_code
            print response.text

        return False

    def check_and_create_index(self, mapping_file_path):
        # Check and create index
        if self.index_exists():
            # Prompt to delete & recreate index
            input = raw_input("Delete and recreate index, " + self.index + "? (y/n)")
            if input in ['y', 'yes']:
                self.delete_index()
                print 'Waiting...'
                time.sleep(5)
                self.create_index(mapping_file_path)
        else:
            # Create Index
            self.create_index(mapping_file_path)

    def create_index(self, mapping_file_path):
        mapping = self.load_mapping_from_file(mapping_file_path)
        self.create_index_from_mapping(mapping)

    def create_index_from_mapping(self, mapping):
        if not self.index_exists():
            print 'Index does not exist, creating index with mapping from file'
            self.put_mapping(mapping)
        else:
            print 'Index already exists, skipping index creation'

    def index_exists(self):
        url = self.server + '/' + self.index + '/' + self.type + '/_search'

        print('Checking index ' + url)
        response = self.session.get(url, auth=self.auth)
        print(str(response.status_code))
        if response.status_code == 200 or response.status_code == 201:
            return True

        return False

    def load_mapping_from_file(self, mapping_file_path):
        print('Loading mapping...')
        with open(mapping_file_path) as mapping_file:
            mapping = json.load(mapping_file)
            pprint('Mapping')
            pprint(mapping)

            return mapping

    def update_mapping(self, mapping):
        print('Adding mapping to index ' + str(self.index))
        url = self.server + '/' + self.index + '/' + '_mapping' + '/' + self.type
        print(url)
        response = self.session.put(url, json=mapping, auth=self.auth)
        print(str(response.text))
        if response.status_code == 200 or response.status_code == 201:
            print('Mapping added to index ' + str(self.index))

    def put_mapping(self, mapping):
        print('Creating index "' + self.index + '" with type "' + self.type + '"')
        url = self.server + '/' + self.index
        response = self.session.put(url, json=mapping, auth=self.auth)
        print(str(response.text))
        if response.status_code == 200:
            print('Index created and mapping added')

    def put_doc(self, _id, doc):
        # print('Indexing doc: ' + _id)
        url = self.server + '/' + self.index + '/' + self.type + '/' + str(_id)
        # print(url)
        response = self.session.put(url, json=doc, auth=self.auth)
        # print(str(response.status_code))
        # print(str(response.text))
        if response.status_code == 200 or response.status_code == 201:
            # print('Saved doc: ' + _id)
            return True

        return False

    def update_doc(self, _id, doc):
        # print('Updating doc: ' + _id)
        url = self.server + '/' + self.index + '/' + self.type + '/' + str(_id) + '/_update'
        # print(url)
        response = self.session.post(url, json=doc, auth=self.auth)
        # print(str(response.status_code))
        # print(str(response.text))
        if response.status_code == 200 or response.status_code == 201:
            # print('Updated doc: ' + _id)
            return True
        else:
            print(str(response.status_code))
            print(str(response.text))

        return False

    def fetch_doc(self, _id, field=None):
        # print('Fetching doc: ' + _id)
        url = self.server + '/' + self.index + '/' + self.type + '/' + str(_id)
        if field is not None:
            url += '?_source=' + field

        # print 'Fetching doc', url
        response = self.session.get(url, auth=self.auth)
        # print(str(response.status_code))
        # print(str(response.text))
        if response.status_code == 200 or response.status_code == 201:
            return json.loads(response.text)
        else:
            pass
            # print(str(response.text))

        return None

    def get_mapping_from_server(self):
        url = self.server + '/' + self.index + '/' + self.type + '/_mapping'

        print('Fetching mapping ' + url)
        response = self.session.get(url, auth=self.auth)
        print(str(response.status_code))
        if response.status_code == 200 or response.status_code == 201:
            mapping = json.loads(response.text)
            if self.index in mapping:
                mapping = mapping[self.index]
                return mapping
        else:
            print(str(response.text))

        return None

    def compare_mapping_from_file(self, mapping_file_path):
        mapping_from_file = self.load_mapping_from_file(mapping_file_path)
        mapping_from_server = self.get_mapping_from_server()

        errors = self.compare_mappings(mapping_from_file, mapping_from_server, [])

        if len(errors) == 0:
            print
            'No errors'
        else:
            pprint(errors)

            # TODO - Update server mapping with missing mapping elements
            # TODO - Or decide if reindexing is needed to update mapping

    # Compares mapping1 and mapping2 and returns differences
    def compare_mappings(self, mapping1, mapping2, path):
        errors = []
        for key in mapping1:
            key_path = path[:]
            key_path.append(key)

            if key in mapping2:
                sub_mapping1 = mapping1[key]
                sub_mapping2 = mapping2[key]

                if isinstance(sub_mapping1, dict) and isinstance(sub_mapping2, dict):
                    sub_errors = self.compare_mappings(sub_mapping1, sub_mapping2, key_path)
                    errors.extend(sub_errors)
                else:
                    if sub_mapping1 != sub_mapping2:
                        errors.append(key_path)
            else:
                print
                'Error: ' + key + ' does not exist'
                errors.append(key_path)

        return errors

    def delete_index(self):
        print 'Deleting index', self.index
        url = self.server + '/' + self.index

        response = self.session.delete(url, auth=self.auth)
        print(str(response.status_code))
        if response.status_code == 200 or response.status_code == 201:
            print(str(response.text))
            return True
        else:
            print(str(response.text))
            return False
