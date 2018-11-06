from data_load.base.data_extractor import DataExtractor
from config import *


class CTDataExtractor(DataExtractor):

    @staticmethod
    def extract_id(data_source_name, row):
        _id = row[ID_FIELD]
        return _id

    @staticmethod
    def extract_data(_id, data_source_name, row):
        doc = {}
        if data_source_name == 'ct_clinical_studies':
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
