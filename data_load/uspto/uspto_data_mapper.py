from data_load.base.data_mapper import DataMapper
import datetime
import re
import time
import json

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

    ##########################################################################################
    #-----------------------------------------CLAIMS-----------------------------------------#
    ##########################################################################################

    @staticmethod
    def process_claims(data):
        if 'us-patent-grant' in data:
            if 'claims' in data['us-patent-grant']:
                if 'claim' in data['us-patent-grant']['claims']:
                    claims = data['us-patent-grant']['claims']['claim']
                    if isinstance(claims, dict):
                        claims = [claims] 
    
                    for claim in claims:
                        if isinstance(claim, dict) and 'claim-text' in claim:
                            claim_text = claim['claim-text']
                            processed_claim_text = USPTODataMapper.process_claim(claim_text)
                            claim['claim-text'] = processed_claim_text

                    data['us-patent-grant']['claims']['claim'] = claims
        return data

    @staticmethod
    def process_claim(claim_text):
        processed_text = ''
        if isinstance(claim_text, list):
            for claim_text_item in claim_text:
                processed_text += USPTODataMapper.process_claim(claim_text_item)
        elif isinstance(claim_text, dict):
            if '#text' in claim_text:
                processed_text += claim_text['#text']
            if 'claim-text' in claim_text:
                processed_text += USPTODataMapper.process_claim(claim_text['claim-text'])
        else:
            if claim_text is not None:
                processed_text += claim_text
        
        return processed_text

    ##########################################################################################
    #----------------------------------US SEQUENCE LIST--------------------------------------#
    ##########################################################################################

    @staticmethod
    def process_us_sequence_list(data):
        if 'us-patent-grant' in data:
            if 'us-sequence-list-doc' in data['us-patent-grant']:
                processed_sequence_list = {}

                us_sequence_list = data['us-patent-grant']['us-sequence-list-doc']
                for key in us_sequence_list:
                    if key == 'p':
                        p = us_sequence_list[key]
                        processed_sequence_list[key] = USPTODataMapper.process_us_sequence_list_p(p)
                    else:
                        processed_sequence_list[key] = us_sequence_list[key]

                data['us-patent-grant']['us-sequence-list-doc'] = processed_sequence_list

        return data

    @staticmethod
    def process_us_sequence_list_p(p):
        if isinstance(p, dict):
            p = [p]

        processed_p = []
        for p_item in p:
            processed_p_item = {}
            for key in p_item:
                if key == 'tables':
                    processed_p_item[key] = json.dumps(p_item[key])
                else:
                    processed_p_item[key] = p_item[key]

            processed_p.append(processed_p_item)

        return processed_p

    ##########################################################################################
    #----------------------------------------ABSTRACT----------------------------------------#
    ##########################################################################################

    @staticmethod
    def process_abstract(data):
        if 'us-patent-grant' in data:
            if 'abstract' in data['us-patent-grant']:
                if 'p' in data['us-patent-grant']['abstract']:
                    p = data['us-patent-grant']['abstract']['p']           
                   
                    processed_p = USPTODataMapper.process_abstract_p(p)

                    data['us-patent-grant']['abstract']['p'] = processed_p

        return data

    @staticmethod
    def process_abstract_p(p):
        if isinstance(p, dict):
            p = [p]

        processed_p = []
        for p_item in p:
            processed_p_item = {}
            for key in p_item:
                if key in ['@id', '@num', '#text']:
                    processed_p_item[key] = p_item[key]  
                else:
                    processed_p_item[key] = json.dumps(p_item[key])

            processed_p.append(processed_p_item)

        return processed_p

    ##########################################################################################
    #-------------------------------------DESCRIPTION----------------------------------------#
    ##########################################################################################

    @staticmethod
    def process_description(data):
        if 'us-patent-grant' in data:
            if 'description' in data['us-patent-grant']:
                if 'p' in data['us-patent-grant']['description']:
                    p = data['us-patent-grant']['description']['p']
                    processed_p = []
                    if isinstance(p, dict):
                        p = [p]

                    for item in p:
                        processed_p.append(USPTODataMapper.process_description_p_item(item))

                    data['us-patent-grant']['description']['p'] = processed_p

                if 'description-of-drawings' in data['us-patent-grant']['description']:
                    if 'p' in data['us-patent-grant']['description']['description-of-drawings']:
                        p = data['us-patent-grant']['description']['description-of-drawings']['p']
                        processed_p = []
                        if isinstance(p, dict):
                            p = [p]

                        for item in p:
                            processed_p.append(USPTODataMapper.process_description_p_item(item))

                        data['us-patent-grant']['description']['description-of-drawings']['p'] = processed_p

                if 'heading' in data['us-patent-grant']['description']:
                    heading = data['us-patent-grant']['description']['heading']
                    processed_heading = []
                    if isinstance(heading, dict):
                        heading = [heading]

                    for item in heading:
                        processed_item = {}
                        for key in item:
                            if key in ['@id', '@level', '#text']:
                                processed_item[key] = item[key]
                            else:
                                processed_item[key] = json.dumps(item[key])
                        
                        processed_heading.append(processed_item)

                    data['us-patent-grant']['description']['heading'] = processed_heading
                    
        return data

    @staticmethod
    def process_description_p_item(p_item):
        processed_p_item = {}
        for key in p_item:
            data = p_item[key]
            if key in ['#text', '@num', '@id']:
                processed_p_item[key] = data
            else:
                # process item
                processed_p_item[key] = json.dumps(data)

        return processed_p_item


    @staticmethod
    def process_sequence_cw(data):
        if 'sequence-cwu' in data:
            if 'table' in data['sequence-cwu']:
                table = data['sequence-cwu']['table']
                table = json.dumps(table)
                data['sequence-cwu']['table'] = table

        return data

    @staticmethod
    def create_doc(_id, data_source_name, data):
        doc = {}

        data = data[0]

        data = USPTODataMapper.process_description(data)
        data = USPTODataMapper.process_claims(data)
        data = USPTODataMapper.process_us_sequence_list(data)
        data = USPTODataMapper.process_abstract(data)
        data = USPTODataMapper.process_sequence_cw(data)

        # path = 'us-patent-grant.claims.claim.claim-text.claim-text'.split('.')
        # data = USPTODataMapper.clean_value_for_path(path, data)

        # path = 'us-patent-grant.claims.claim.claim-text'.split('.')
        # data = USPTODataMapper.clean_value_for_path(path, data)

        # us-patent-grant.us-sequence-list-doc.p.tables.table.tgroup.tbody.row.entry

        path = 'us-patent-grant.us-bibliographic-data-grant.us-references-cited.us-citation.nplcit.othercit'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        path = 'us-patent-grant.us-bibliographic-data-grant.us-references-cited.us-citation.nplcit.othercit.sub'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        path = 'us-patent-grant.us-bibliographic-data-grant.us-references-cited.us-citation.nplcit.othercit.sup'.split('.')
        data = USPTODataMapper.clean_value_for_path(path, data)

        # path = 'us-patent-grant.description.p.tables.table.tgroup.tbody.row.entry'.split('.')
        # data = USPTODataMapper.clean_value_for_path(path, data)

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