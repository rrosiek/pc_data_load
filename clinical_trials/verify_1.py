from data_load.base.utils import export_doc_ids
from config import *
import ct_load_config

def run():
    server = SERVER
    load_config = ct_load_config.get_load_config()
    other_files_directory = load_config.other_files_directory()
    ct_v2_ids = export_doc_ids.get_doc_ids(SERVER, 'clinical_trials_v2', 'study', other_files_directory, 'ct_v2_ids.json')
    ct_v1_ids = export_doc_ids.get_doc_ids(SERVER, 'clinical_trials', 'study', other_files_directory, 'ct_v1_ids.json')

    common_ids = {}
    for _id in ct_v2_ids:
        if _id in ct_v1_ids:
            common_ids[_id] = 0

    print 'ct_v1_ids', len(ct_v1_ids)
    print 'ct_v2_ids', len(ct_v2_ids)
    print 'common_ids', len(common_ids)


run()

