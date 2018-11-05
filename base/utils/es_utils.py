import json
import requests

from data_load.base.constants import API_URL

def get_list_of_indexes():
    url = API_URL + 'index/external/all/'
    print 'Getting list of indices', url
    response = requests.get(url)
    if response.status_code == 200:
        response_json = response.json()
        # print response_json

        index_list = response_json['result']

        return index_list

    return []

def get_info_for_index_id(index_id):
    index_list = get_list_of_indexes()
    for index_item in index_list:
        if index_item['index_identifier'] == index_id:
            return index_item

    return None


