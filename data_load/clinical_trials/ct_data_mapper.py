from data_load.base.data_mapper import DataMapper
from config import *
import json
import time

class CTDataMapper(DataMapper):

    @staticmethod
    def allow_doc_creation(data_source_name):
        if data_source_name == 'clinical_study':
            return True

        return False

    @staticmethod
    def create_only(data_source_name):
        if data_source_name == 'clinical_study':
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
        # if data_source_name == 'ct_clinical_studies':
        # for data_item in data:
        #     print json.dumps(data_item)
        #     if 'start_date' in data_item:
        #         start_date = data_item['start_date']
        #         print start_date
        #         start_date_year = start_date[-4:]

        #         doc['startDateYear'] = start_date_year

        #         break

        # if data_source_name == 'ct_clinical_studies':
        if len(data) == 1:
            data = data[0]

        if 'start_date' in data:
            start_date_obj = data['start_date']
            if not isinstance(start_date_obj, dict):
                start_date_obj = {
                    "#text": start_date_obj
                }

            data['start_date'] = start_date_obj

        if 'completion_date' in data:
            start_date_obj = data['completion_date']
            if not isinstance(start_date_obj, dict):
                start_date_obj = {
                    "#text": start_date_obj
                }

            data['completion_date'] = start_date_obj

        if 'primary_completion_date' in data:
            start_date_obj = data['primary_completion_date']
            if not isinstance(start_date_obj, dict):
                start_date_obj = {
                    "#text": start_date_obj
                }

            data['primary_completion_date'] = start_date_obj

        if 'enrollment' in data:
            start_date_obj = data['enrollment']
            if not isinstance(start_date_obj, dict):
                start_date_obj = {
                    "#text": start_date_obj
                }

            data['enrollment'] = start_date_obj

        data = CTDataMapper.clean_value_for_path(["clinical_results","reported_events","serious_events","category_list","category","event_list","event","sub_title"], data)
        data = CTDataMapper.clean_value_for_path(["clinical_results","reported_events","other_events","category_list","category","event_list","event","sub_title"], data)

        # value_for_path = CTDataMapper.get_value_for_path(["clinical_results","reported_events","serious_events","category_list","category","event_list","event","sub_title"], data)
        # if value_for_path is not None:
        #     print value_for_path
        #     if not isinstance(value_for_path, dict):
        #         value_for_path = {
        #             "#text": value_for_path
        #         } 


        #     # print data
        #     CTDataMapper.set_value_for_path(["clinical_results","reported_events","serious_events","category_list","category","event_list","event","sub_title"], data, value_for_path)

        doc = data
        return doc

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
            else:
                cleaned_value = CTDataMapper.clean_value_for_path(path, value)
                if cleaned_value is not None:
                    data[key] = cleaned_value
        elif isinstance(data, list):
            # print data
            cleaned_data = []
            for item in data:
                path_copy = path[:]
                cleaned_item = CTDataMapper.clean_value_for_path(path_copy, item)
                cleaned_data.append(cleaned_item)
                            
            data = cleaned_data

                
        return data


    @staticmethod
    def get_value_for_path(path, data):
        for key in path:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None

        return data
        
    @staticmethod
    def set_value_for_path(path, data, value):
        while len(path) > 1:
            key = path.pop(0)

            if isinstance(data, dict) and key in data:
                data = data[key]

        # print data

        key = path.pop(0)
        if isinstance(data, dict) and key in data:
            data[key] = value

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        doc = {}
        # if data_source_name in existing_doc:
        #     existing_value = existing_doc[data_source_name]
        #     data = CTDataMapper.update_dict_array(existing_value, data)

        # if data_source_name == 'ct_clinical_studies':
        if len(data) == 1:
            data = data[0]

        # doc[data_source_name] = data
        doc = data
        doc = CTDataMapper.merge_dict(existing_doc, doc)

        return doc