from data_load.base.utils.data_loader_utils import DataLoaderUtils
import requests
import json
import data_load.base.utils.file_utils


def duplicate_index(server, src_index, src_type, dst_index, dst_type, mapping=None):
    src_data_loader_utils = DataLoaderUtils(server, src_index, src_type)
    dest_data_loader_utils = DataLoaderUtils(server, dst_index, dst_type)

    if mapping is None:
        # Get mapping from src index
        mapping = src_data_loader_utils.get_mapping_from_server()

    if not dest_data_loader_utils.index_exists():
        print 'Creating index'
        dest_data_loader_utils.put_mapping(mapping)
    else:
        print dst_index, 'exists'

    data = {
        "source": {
            "index": src_index
        },
        "dest": {
            "index": dst_index
        }
    }

    url = server + '/_reindex?wait_for_completion=false'

    print url
    print data

    response = requests.post(url, json=data)

    print response
    print json.loads(response.text)


def run():
    server = 'http://localhost:9200'
    src_index = 'clinical_trials'
    src_type = 'study'

    dst_index = 'clinical_trials_backup'
    dst_type = 'study'

    mapping_file = None

    mapping = None
    if mapping_file is not None:
        mapping = data_load.base.utils.file_utils.load_file_path(mapping_file)

    duplicate_index(server, src_index, src_type, dst_index, dst_type, mapping)


run()










