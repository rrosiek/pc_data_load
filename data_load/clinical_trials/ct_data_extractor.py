from data_load.base.data_extractor import DataExtractor
from config import *
import time
import json

class CTDataExtractor(DataExtractor):

    @staticmethod
    def extract_id(data_source_name, row):
        # print json.dumps(row)
        # time.sleep(10)
        if 'id_info' in row:
            if ID_FIELD in row['id_info']:
                _id = row['id_info'][ID_FIELD]
                return _id

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        doc = {}
        if data_source_name == 'clinical_study':
            doc = row
        elif data_source_name == 'ct_references':
            # print row
            # raw_input('Continue?')
            doc = row['pmid']
        else:
            doc = dict(row)
            doc.pop('nct_id', None)
            doc.pop('id', None)

        return doc
