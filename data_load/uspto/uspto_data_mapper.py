from data_load.base.data_mapper import DataMapper
import datetime
import re
import time

class USPTODataMapper(DataMapper):
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
        number_str = ''
        while len(number) < count:
            number_str = '0' + number_str

        return number_str

    @staticmethod
    def clean_date(date_str):
        comps = date_str.split('/')
        if len(comps) == 3:
            month = USPTODataMapper.pad_zeros(comps[0], 2)
            day = USPTODataMapper.pad_zeros(comps[1], 2)
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

        path = 'us-patent-grant.claims.claim.claim-text.claim-text'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        path = 'us-patent-grant.claims.claim.claim-text'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        path = 'us-patent-grant.us-bibliographic-data-grant.us-references-cited.us-citation.nplcit.othercit'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        path = 'us-patent-grant.us-bibliographic-data-grant.us-references-cited.us-citation.nplcit.othercit.sup'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        path = 'us-patent-grant.description.p.tables.table.tgroup.tbody.row.entry'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        doc = data # USPTODataMapper.add_value_for_path('us-patent-grant.us-bibliographic-data-grant', data, doc)
        
        # print doc
        # raw_input('Continue?')

        return doc

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        new_doc = USPTODataMapper.create_doc(_id, data_source_name, data)
        update_doc = new_doc

        return update_doc

    @staticmethod
    def add_value_for_path(path, src, dst):
        key = path[0]

        if isinstance(src, dict) and key in src:
            key = path.pop(0)
            value = src[key]
            sub_dst = {}
            dst[key] = sub_dst

            if len(path) == 0:
                if not isinstance(value, dict):
                    value = {
                        "#text": value
                    }
                
                dst[key]= value
                # print key, value
            else:
                cleaned_value = USPTODataMapper.add_value_for_path(path, value, sub_dst)
                if cleaned_value is not None:
                    dst[key] = cleaned_value
        elif isinstance(src, list):
            # print data
            cleaned_data = []
            for item in src:
                path_copy = path[:]
                cleaned_item = USPTODataMapper.add_value_for_path(path_copy, item, dst)
                cleaned_data.append(cleaned_item)
                            
            dst = cleaned_data

        return dst

    @staticmethod
    def clean_value_for_path(path, data):
        key = path[0]
        if isinstance(data, dict) and key in data:
            key = path.pop(0)
            value = data[key]

            if len(path) == 0:
                if not isinstance(value, dict):
                    value = {
                        "#text": value
                    }
                
                data[key] = value
                # print key, value
            else:
                cleaned_value = USPTODataMapper.clean_value_for_path(path, value)
                if cleaned_value is not None:
                    data[key] = cleaned_value
        elif isinstance(data, list):
            # print data
            cleaned_data = []
            for item in data:
                path_copy = path[:]
                cleaned_item = USPTODataMapper.clean_value_for_path(path_copy, item)
                cleaned_data.append(cleaned_item)
                            
            data = cleaned_data

                
        return data