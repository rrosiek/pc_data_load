import time

class DataMapper(object):

    @staticmethod
    def allow_doc_creation(data_source_name):
        return False

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
    def create_doc(_id, data_source_name, data):
        doc = {}
        return doc

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        doc = {}
        return doc

    @staticmethod
    def update_dict_array(existing_value, new_value):
        if isinstance(existing_value, list) and len(existing_value) > 0:
            for existing_item in existing_value:
                match = False
                for item in new_value:
                    if DataMapper.compare_dict(item, existing_item):
                        match = True
                        break

                if not match:
                    new_value.append(existing_item)
        else:
            match = False
            for item in new_value:
                if DataMapper.compare_dict(item, existing_value):
                    match = True
                    break

            if not match:
                new_value.append(existing_value)

        return new_value

    @staticmethod
    def update_str_array(existing_value, new_value):
        if isinstance(existing_value, list) and len(existing_value) > 0:
            for existing_item in existing_value:
                match = False
                for item in new_value:
                    if item == existing_item:
                        match = True
                        break

                if not match:
                    new_value.append(existing_item)
        else:
            match = False
            for item in new_value:
                if item == existing_value:
                    match = True
                    break

            if not match:
                new_value.append(existing_value)

        return new_value

    @staticmethod
    def compare_dict(dict1, dict2):
        match = True
        if isinstance(dict1, list) and isinstance(dict2, list):
            if len(dict1) == len(dict2):
                index = 0
                for dict_item1 in dict1:
                    dict_item2 = dict2[index]
                    index += 1

                    if not DataMapper.compare_dict(dict_item1, dict_item2):
                        match = False
                        break
            else:
                match = False
        elif isinstance(dict1, dict) and isinstance(dict2, dict):
            if len(dict1) != len(dict2):
                match = False
            else:
                for key in dict1:
                    dict1_value = dict1[key]
                    if key in dict2:
                        dict2_value = dict2[key]
                        if not DataMapper.compare_dict(dict1_value, dict2_value):
                            match = False
                            break
                    else:
                        match = False
                        break
        elif isinstance(dict1, str) and isinstance(dict2, str):
            if dict1 != dict2:
                match = False
        else:
            match = False

        return match

    @staticmethod
    def clean_up_relations(relations):
        if DataMapper.verify_relations_dict(relations):
            relations = [relations]

        cleaned_relations = []
        for relation in relations:
            if DataMapper.verify_relations_dict(relation):
                cleaned_relations.append(relation)

        return cleaned_relations


    @staticmethod
    def update_citations_for_doc(_id, doc, dest_ids, source, index_id, append=True):
        citations_for_index = {
            'index_id': index_id,
            'source': source,
            'ids': dest_ids
        }

        citations = None
        if 'citations' in doc:
            citations = doc['citations']
            citations = DataMapper.clean_up_relations(citations)
        
        try:
            if citations is not None and len(citations) > 0:
                # Iterate and find citations
                citations_for_index_found = False
                for citation in citations:
                    if citation['index_id'] == index_id and citation['source'] == source:
                        if append:
                            citation_ids = citation['ids']
                            for citation_id in citation_ids:
                                citation_id = str(citation_id)
                                if citation_id not in dest_ids:
                                    dest_ids.append(citation_id)

                        citation['ids'] = dest_ids
                        citations_for_index_found = True
                        break
                if not citations_for_index_found:
                    citations.append(citations_for_index)
            else:
                citations = [citations_for_index]

            doc['citations'] = citations
        except Exception as e:
            print e
            print doc
            print citations
            # time.sleep(100)

        return doc

    @staticmethod
    def update_cited_bys_for_doc(_id, doc, dest_ids, source, index_id, append=True):

        cited_bys_for_index = {
            'index_id': index_id,
            'source': source,
            'ids': dest_ids
        }

        cited_bys = None
        if 'cited_bys' in doc:
            cited_bys = doc['cited_bys']
            cited_bys = DataMapper.clean_up_relations(cited_bys)

        try:
            if cited_bys is not None and len(cited_bys) > 0:
                # Iterate and find citations
                cited_bys_for_index_found = False
                for cited_by in cited_bys:
                    if cited_by['index_id'] == index_id and cited_by['source'] == source:
                        if append:
                            cited_by_ids = cited_by['ids']
                            for cited_by_id in cited_by_ids:
                                cited_by_id = str(cited_by_id)
                                if cited_by_id not in dest_ids:
                                    dest_ids.append(cited_by_id)

                        cited_by['ids'] = dest_ids
                        cited_bys_for_index_found = True
                        break
                if not cited_bys_for_index_found:
                    cited_bys.append(cited_bys_for_index)
            else:
                cited_bys = [cited_bys_for_index]

            doc['cited_bys'] = cited_bys
        except Exception as e:
            print e
            print doc
            print cited_bys
            # time.sleep(100)

        return doc

    @staticmethod
    def update_relations_for_doc(_id, doc, dest_ids, source, index_id, append=True):
        relations_for_index = {
            'index_id': index_id,
            'source': source,
            'ids': dest_ids
        }

        relations = None
        if 'relations' in doc:
            relations = doc['relations']

            relations = DataMapper.clean_up_relations(relations)

        if relations is not None and len(relations) > 0:
            # Iterate and find citations
            relations_for_index_found = False
            for relation in relations:
                if relation['index_id'] == index_id and relation['source'] == source:
                    if append:
                        relation_ids = relation['ids']
                        for relation_id in relation_ids:
                            relation_id = str(relation_id)
                            if relation_id not in dest_ids:
                                dest_ids.append(relation_id)

                    relation['ids'] = dest_ids
                    relations_for_index_found = True
                    break
            if not relations_for_index_found:
                relations.append(relations_for_index)
        else:
            relations = [relations_for_index]

        doc['relations'] = relations

        return doc

    @staticmethod
    def remove_newlines(value):
        value = value.replace('\\n', '')
        value = value.replace('\\r', '')
        value = value.replace('\n', '')
        value = value.replace('\r', '')

        value = value.strip() 

        return value

    @staticmethod
    def add_values_for_key(keys, doc, data_item):
        for key in keys:
            doc = DataMapper.add_value_for_key(key, doc, data_item)
        
        return doc

    @staticmethod
    def add_value_for_key(key, doc, data_item):
        value = None

        if key in data_item:
            value = data_item[key]
        
        if key.upper() in data_item:
            value = data_item[key.upper()]

        if key.lower() in data_item:
            value = data_item[key.lower()]

      
        if value is not None and len(value) > 0:
            value = DataMapper.remove_newlines(value)
            # value = unicode(value, errors='ignore')
            doc[key] = value.decode("utf-8", "ignore").encode('utf8')

        return doc


    @staticmethod
    def process_relationships(self, load_config, data_rows):
        # return {
        #     "SOURCE_INDEX_ID": {
        #         "es_id": [
        #             {
        #                 "index_id": "DEST_INDEX_ID",
        #                 "ids": [],
        #                 "type": "RELATIONSHIP_TYPE"
        #             }
        #         ]
        #     }
        # }
        return {}


    @staticmethod
    def reformat(reformatted_array, relations_array, dest_index_id, relationship_type):
        for _id in relations_array:
            if _id not in reformatted_array:
                reformatted_array[_id] = []

            relationship = {
                'index_id': dest_index_id,
                'ids': relations_array[_id],
                'type': relationship_type
            }

            reformatted_array[_id].append(relationship)

        return reformatted_array

    @staticmethod
    def merge_dict(existing_dict, new_dict):
        if existing_dict is None or len(existing_dict) == 0:
            return new_dict

        if new_dict is None or len(new_dict) == 0:
            return existing_dict

        all_keys = []
        all_keys.extend(existing_dict.keys())
        all_keys.extend(new_dict.keys())

        merged_dict = {}

        for key in all_keys:
            existing_value = None
            new_value = None
            if key in existing_dict:
                existing_value = existing_dict[key]

            if key in new_dict:
                new_value = new_dict[key]

            if isinstance(existing_value, dict) and isinstance(new_value, dict):
                new_value = DataMapper.merge_dict(existing_value, new_value)
            if isinstance(existing_value, list) and isinstance(new_value, list):
                new_value = DataMapper.merge_list(existing_value, new_value)
            if new_value is None:
                new_value = existing_value
            elif not isinstance(new_value, bool) and len(new_value) == 0:
                new_value = existing_value

            if new_value is not None and isinstance(new_value, bool):
                merged_dict[key] = new_value
            elif new_value is not None and len(new_value) > 0:
                merged_dict[key] = new_value

        return merged_dict

    @staticmethod
    def merge_list(existing_list, new_list):
        for item in new_list:
            if not DataMapper.list_contains_item(existing_list, item):
                existing_list.append(item)

        return existing_list

    @staticmethod
    def list_contains_item(list_of_items, item):
        for other_item in list_of_items:
            if DataMapper.compare_dict(item, other_item):
                return True

        return False


    @staticmethod
    def verify_relations_dict(relations_dict):
        if isinstance(relations_dict, dict):
            if 'index_id' in relations_dict and 'source' in relations_dict and 'ids' in relations_dict:
                return True

        return False
