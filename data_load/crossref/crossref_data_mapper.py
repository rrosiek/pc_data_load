from data_load.base.data_mapper import DataMapper
import datetime
import re
import time

class CrossrefDataMapper(DataMapper):
    @staticmethod
    def allow_doc_creation(data_source_name):
        return True

    @staticmethod
    def create_only(data_source_name):
        return False

    @staticmethod
    def get_es_id(_id):
        return _id

    @staticmethod
    def get_doc_id(_id):
        return _id

    @staticmethod
    def pad_zeros(number, count):
        number_str = str(number)
        while len(number_str) < count:
            number_str = '0' + number_str

        return number_str

    @staticmethod
    def clean_date(date_str):
        comps = date_str.split('/')
        if len(comps) == 3:
            month = CrossrefDataMapper.pad_zeros(comps[0], 2)
            day = CrossrefDataMapper.pad_zeros(comps[1], 2)
            year = comps[2]
            return str(month) + '/' + str(day) + '/' + str(year)
        else:
            return None

    @staticmethod
    def add_value_if_not_null(doc, data, key):
        if key in data:
            value = data[key]
            if value != 'NULL' and len(value) > 0:
                doc[key] = value

        return doc

    @staticmethod
    def create_doc(_id, data_source_name, data):
        doc = {}

        data = data[0]

        doc = data

        return doc

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        new_doc = CrossrefDataMapper.create_doc(_id, data_source_name, data)
        update_doc = new_doc

        return update_doc
