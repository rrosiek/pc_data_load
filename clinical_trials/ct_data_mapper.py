from data_load.base.data_mapper import DataMapper
from config import *


class CTDataMapper(DataMapper):

    @staticmethod
    def allow_doc_creation(data_source_name):
        if data_source_name == 'ct_clinical_studies':
            return True

        return False

    @staticmethod
    def create_only(data_source_name):
        if data_source_name == 'ct_clinical_studies':
            return True

        return False

    @staticmethod
    def get_es_id(_id):
        return _id

    @staticmethod
    def get_doc_id(_id):
        return _id

    @staticmethod
    def create_doc(_id, data_source_name, data):
        doc = {}
        if data_source_name == 'ct_clinical_studies':
            for data_item in data:
                if 'start_date' in data_item:
                    start_date = data_item['start_date']
                    start_date_year = start_date[-4:]

                    doc['startDateYear'] = start_date_year

                    break

        if data_source_name == 'ct_clinical_studies':
            if len(data) == 1:
                data = data[0]

        doc[data_source_name] = data
        return doc

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        doc = {}
        # if data_source_name in existing_doc:
        #     existing_value = existing_doc[data_source_name]
        #     data = CTDataMapper.update_dict_array(existing_value, data)

        if data_source_name == 'ct_clinical_studies':
            if len(data) == 1:
                data = data[0]

        doc[data_source_name] = data
        doc = CTDataMapper.merge_dict(existing_doc, doc)

        return doc