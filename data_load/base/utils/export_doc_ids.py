from data_utils import DataUtils
import file_utils


def export_doc_ids(server, src_index, src_type):
    print __name__, 'Fetching doc ids for', server, src_index, src_type
    query = {
            "match_all": {}
    }
    data_utils = DataUtils()
    ids = data_utils.batch_fetch_ids_for_query(base_url=server, index=src_index, type=src_type, query=query)

    documents_ids = dict.fromkeys(ids, None)
    print __name__, 'Done, fetched', len(documents_ids), 'doc ids'

    return documents_ids

def get_doc_ids(server, src_index, src_type, dest_dir, dest_file_name):
    documents_ids = file_utils.load_file(dest_dir, dest_file_name)

    if len(documents_ids) == 0:
        documents_ids = export_doc_ids(server, src_index, src_type)

        print __name__, 'Saving to', dest_dir, dest_file_name
        file_utils.make_directory(dest_dir)
        file_utils.save_file(dest_dir, dest_file_name, documents_ids)

    return documents_ids
    
def get_doc_ids_for_load_config(load_config):
    dest_dir = load_config.other_files_directory()
    dest_file_name = 'DOC_IDS_' + load_config.index + '.json'
    documents_ids = get_doc_ids(load_config.server, load_config.index, load_config.type, dest_dir, dest_file_name)
    return documents_ids

def run():
    server = 'http://localhost:9200'
    src_index = 'pubmed2018'
    src_type = 'article'

    get_doc_ids(server, src_index, src_type, '', 'all_pubmed_ids.json')

