import requests
import json


PROCESS_IDS_BATCH_SIZE = 1000
PROCESS_QUERY_BATCH_SIZE = 1000

session = None


def docs_for_tag_query(tag, email, client_identifier):
    data = {
        "nested": {
            "path": "userTags",
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "userTags.added_by": email
                            }
                        },
                        {
                            "match": {
                                "userTags.tag": tag
                            }
                        },
                        {
                            "match": {
                                "userTags.client": client_identifier
                            }
                        }
                    ]
                }
            }
        }
    }

    return data


def get_page_size(start_index, page_size, total_size):
    if start_index + page_size > total_size:
        page_size = total_size - start_index
    return page_size


def extract_ids_from_docs(docs):
    ids = []
    for doc in docs:
        ids.append(doc["_id"])

    return ids


def batch_process_docs_for_ids(base_url, ids, index, type, process_doc, batch_size=PROCESS_IDS_BATCH_SIZE, username='', password=''):
    size = len(ids)

    start_index = 0
    page_size = get_page_size(start_index, batch_size, size)

    processed_docs = []

    while start_index < size:
        ids_slice = ids[start_index: start_index + page_size]
        docs = fetch_docs_for_ids(base_url, ids_slice, index, type, username, password)

        for doc in docs:
            processed_docs.append(process_doc(doc, index, type))

        start_index += page_size
        page_size = get_page_size(start_index, batch_size, size)

    return processed_docs


def batch_fetch_docs_for_ids(base_url, ids, index, type, docs_fetched=None, batch_size=PROCESS_IDS_BATCH_SIZE, username='', password=''):
    size = len(ids)

    start_index = 0
    page_size = get_page_size(start_index, batch_size, size)

    # all_docs = []

    while start_index < size:
        ids_slice = ids[start_index: start_index + page_size]
        docs = fetch_docs_for_ids(base_url, ids_slice, index, type, username, password)

        # all_docs.extend(docs)
        if docs_fetched is not None:
            docs_fetched(docs, index, type)

        start_index += page_size
        page_size = get_page_size(start_index, batch_size, size)

    # return all_docs


def batch_fetch_ids_for_query(base_url, query, index, type, ids_fetched=None, batch_size=PROCESS_QUERY_BATCH_SIZE):
    url = start_scroll_request_url(base_url, 40, index, type)

    # print url

    query = {
        'query': query,
        "stored_fields": [],
        'size': batch_size
        # Set to 1000, Cannot be set to a higher value, the search context might
        # expire by the time the large number of docs are processed.
    }

    # print query

    results_fetched = 0
    response = fetch_docs_for_query(url, query)

    all_ids = []
    if response is not None:
        scroll_id, hits, size = process_response(response)
        # print ('Scroll Id:', scroll_id)
        results_fetched += len(hits)
        # print ('Results fetched:', results_fetched, '/', size)

        ids = extract_ids_from_docs(hits)

        all_ids.extend(ids)
        if ids_fetched is not None:
            ids_fetched(ids, index, type)

        while len(hits) > 0:
            url = scroll_request_url(base_url)
            query = {
                "scroll": "10m",
                "scroll_id": scroll_id
            }

            response = fetch_docs_for_query(url, query)
            hits = []
            if response is not None:
                scroll_id, hits, size = process_response(response)
                # print ('Scroll Id:', scroll_id)
                results_fetched += len(hits)
                # print ('Results fetched:', results_fetched, '/', size)

                ids = extract_ids_from_docs(hits)

                all_ids.extend(ids)
                if ids_fetched is not None:
                    ids_fetched(ids, index, type)

    return all_ids


def process_response(response):
    scroll_id = response['_scroll_id']

    hits = []
    size = 0
    if 'hits' in response:
        outer_hits = response['hits']
        size = outer_hits['total']
        if 'hits' in outer_hits:
            hits = outer_hits['hits']
        else:
            print ('No inner hits')
    else:
        print ('No outer hits')

    return scroll_id, hits, size


def fetch_docs_for_query(url, query, username='', password=''):
    global session
    if session is None:
        session = requests.session()
    response = session.post(url, json=query, auth=(username, password))
    # print response

    if response.status_code == 200:
        # print (response.status_code)
        response_obj = json.loads(response.text)
        return response_obj
    else:
        print (response.status_code)
        print (response.text)

    return None


def start_scroll_request_url(base_url, duration, index=None, type=None):
    url = base_url

    if index is not None:
        url += '/' + index

    if type is not None:
        url += '/' + type

    url += '/_search?scroll=' + str(duration) + 'm'

    return url


def scroll_request_url(base_url):
    url = base_url + '/_search/scroll'

    return url


def fetch_docs_for_ids(base_url, ids, index, type, username='', password=''):
    global session
    if session is None:
        session = requests.session()

    url = base_url + '/' + index + '/' + type + "/_mget"
    data = {
        "ids": ids
    }

    # print(url)
    # print len(ids)
    # print data
    try:
        response = session.post(url, json=data, auth=(username, password))
        # print(response)

        if response.status_code == 200:
            # print response.status_code
            response_obj = json.loads(response.text)
            if 'docs' in response_obj:
                return response_obj['docs']
        else:
            print (response.status_code)
            print (response.text)
    except Exception as e:
        print e
        return []

    return []

def get_total_doc_count(base_url, index, type):
    global session
    if session is None:
        session = requests.session()

    url = base_url + '/' + index + '/' + type + "/_search?size=0"
    # print url

    try:
        response = session.get(url)
        # print(response)

        if response.status_code == 200:
            # print response.status_code
            response_obj = json.loads(response.text)
            if 'hits' in response_obj:
                hits = response_obj['hits']
                if 'total' in hits:
                    total = hits['total']
                    return total
        else:
            print (response.status_code)
            print (response.text)
    except Exception as e:
        print e
        return 0
