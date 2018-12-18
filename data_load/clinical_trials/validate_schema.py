
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
# from json_schema import json_schema
# from json_schema.json_differ import diff_jsons
from data_load.xsdtojson.lib import xsd_to_json_schema

from data_load.base.utils import file_utils

# from jsonmerge import Merger

import xmltodict
import json
import time
import os

from jsonschema import validate


doc_json_str = '{"clinical_results":{"participant_flow":{"recruitment_details":"","group_list":{"group":{"@group_id":"P1","title":"Antineoplaston Therapy","description":""}},"period_list":{"period":{"title":"Overall Study","milestone_list":{"milestone":[{"title":"STARTED","participants_list":{"participants":{"@group_id":"P1","@count":"9"}}}]},"drop_withdraw_reason_list":{"drop_withdraw_reason":{"title":"Not evaluable","participants_list":{"participants":{"@group_id":"P1","@count":"3"}}}}}}},"baseline":{"group_list":{"group":{"@group_id":"B1","title":"Antineoplaston Therapy","description":""}},"analyzed_list":{"analyzed":{"units":"Participants","scope":"Overall","count_list":{"count":{"@group_id":"B1","@value":"9"}}}},"measure_list":{"measure":[{"title":"Sex: Female, Male","units":"Participants","param":"Count of Participants","class_list":{"class":{"category_list":{"category":[{"title":"Female","measurement_list":{"measurement":{"@group_id":"B1","@value":"5"}}}]}}}}]}},"outcome_list":{"outcome":[{"type":"Primary","title":"Number of Participants With Objective Response","description":"","group_list":{"group":{"@group_id":"O1","title":"Antineoplaston Therapy","description":""}},"measure":{"title":"Number of Participants With Objective Response","description":"","units":"Participants","param":"Number","analyzed_list":{"analyzed":{"units":"Participants","scope":"Measure","count_list":{"count":{"@group_id":"O1","@value":"6"}}}},"class_list":{"class":[{"title":"Progressive Disease","category_list":{"category":{"measurement_list":{"measurement":{"@group_id":"O1","@value":"3"}}}}}]}}},{"type":"Secondary","title":"Percentage of Participants Who Survived","description":"6 months, 12 months, 24 months, 36 months, 48 months, 60 months overall survival","time_frame":"6 months, 12 months, 24 months, 36 months, 48 months, 60 months","population":"All study subjects receiving any Antineoplaston therapy","group_list":{"group":{"@group_id":"O1","title":"Antineoplaston Therapy","description":""}},"measure":{"title":"Percentage of Participants Who Survived","description":"6 months, 12 months, 24 months, 36 months, 48 months, 60 months overall survival","population":"All study subjects receiving any Antineoplaston therapy","units":"Percentage of Participants","param":"Number","analyzed_list":{"analyzed":{"units":"Participants","scope":"Measure","count_list":{"count":{"@group_id":"O1","@value":"9"}}}},"class_list":{"class":[{"title":"60 months overall survival","category_list":{"category":{"measurement_list":{"measurement":{"@group_id":"O1","@value":"11.1"}}}}}]}}}]},"reported_events":{"time_frame":"4 years, 2 months","desc":"","group_list":{"group":{"@group_id":"E1","title":"Antineoplaston Therapy","description":""}},"serious_events":{"default_vocab":"CTCAE (3.0)","default_assessment":"Systematic Assessment","category_list":{"category":[{"title":"Total","event_list":{"event":{"sub_title":{"#text":"Total, serious adverse events"},"counts":{"@group_id":"E1","@subjects_affected":"5","@subjects_at_risk":"9"}}}},{"title":"Blood and lymphatic system disorders","event_list":{"event":{"sub_title":{"@vocab":"CTCAE (Version 3","#text":"Hemoglobin"},"description":"The Hemoglobin was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}},{"title":"Gastrointestinal disorders","event_list":{"event":[{"sub_title":{"#text":"Pancreatitis"},"description":"The Pancreatitis was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":{"#text":"Pain: Stomach"},"description":"The Pain: Stomach was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"Infections and infestations","event_list":{"event":[{"sub_title":{"@vocab":"Institutional","#text":"Central venous catheter infection"},"description":"The Central venous catheter infection was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":{"#text":"Infection (documented clinically): Blood"},"description":"The Infection (documented clinically): Blood was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"Investigations","event_list":{"event":{"sub_title":{"#text":"Hypokalemia"},"description":"The Hypokalemia was possibly related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}},{"title":"Musculoskeletal and connective tissue disorders","event_list":{"event":{"sub_title":{"#text":"Pain: Joint"},"description":"The Pain: Joint was possibly related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}},{"title":"Nervous system disorders","event_list":{"event":[{"sub_title":{"#text":"Confusion"},"description":"The Confusion was possibly related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":{"#text":"Seizure"},"description":"The Seizure was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":{"#text":"Somnolence/depressed level of consciousness"},"description":"The Somnolence/depressed level of consciousness was not related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"Skin and subcutaneous tissue disorders","event_list":{"event":{"sub_title":{"#text":"Rash: erythema multiforme"},"description":"The Rash: erythema multiforme was possibly related to Antineoplaston therapy.","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}}]}},"other_events":{"frequency_threshold":"5","default_vocab":"CTCAE (3.0)","default_assessment":"Systematic Assessment","category_list":{"category":[{"title":"Total","event_list":{"event":{"sub_title":"Total, other adverse events","counts":{"@group_id":"E1","@subjects_affected":"9","@subjects_at_risk":"9"}}}},{"title":"Blood and lymphatic system disorders","event_list":{"event":[{"sub_title":"Hemoglobin","counts":{"@group_id":"E1","@subjects_affected":"5","@subjects_at_risk":"9"}},{"sub_title":"Leukocytes (total WBC)","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Lymphopenia","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Neutrophils/granulocytes (ANC/AGC)","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"Gastrointestinal disorders","event_list":{"event":[{"sub_title":"Diarrhea","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Nausea","counts":{"@group_id":"E1","@subjects_affected":"5","@subjects_at_risk":"9"}},{"sub_title":"Vomiting","counts":{"@group_id":"E1","@subjects_affected":"4","@subjects_at_risk":"9"}},{"sub_title":"Pancreatitis","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Pain: Dental/teeth/peridontal","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Pain: Stomach","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"General disorders","event_list":{"event":[{"sub_title":{"@vocab":"Institutional","#text":"Non-functional central venous catheter"},"counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Fatigue (asthenia, lethargy, malaise)","counts":{"@group_id":"E1","@subjects_affected":"4","@subjects_at_risk":"9"}},{"sub_title":"Fever","counts":{"@group_id":"E1","@subjects_affected":"2","@subjects_at_risk":"9"}},{"sub_title":"Insomnia","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Rigors/chills","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Weight gain","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Pruritus/itching","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":{"@vocab":"Institutional","#text":"Edema/Fluid retention"},"counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"Immune system disorders","event_list":{"event":{"sub_title":"Allergic reaction/hypersensitivity (including drug fever)","counts":{"@group_id":"E1","@subjects_affected":"2","@subjects_at_risk":"9"}}}},{"title":"Infections and infestations","event_list":{"event":[{"sub_title":{"@vocab":"Institutional","#text":"Central venous catheter infection"},"counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Bladder (urinary)","counts":{"@group_id":"E1","@subjects_affected":"2","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Blood","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Mucosa","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Pharynx","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Sinus","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Soft tissue NOS","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Infection (documented clinically): Urinary tract NOS","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Middle ear (otitis media)","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Skin","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Upper airway","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}]}},{"title":"Investigations","event_list":{"event":[{"sub_title":"Hyperglycemia","counts":{"@group_id":"E1","@subjects_affected":"2","@subjects_at_risk":"9"}},{"sub_title":"Hypernatremia","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Hyperuricemia","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Hypocalcemia","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Hypochloremia","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Hypoglycemia","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Hypokalemia","counts":{"@group_id":"E1","@subjects_affected":"7","@subjects_at_risk":"9"}},{"sub_title":"Metabolic/Laboratory - Other","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Proteinuria","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"SGOT","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"SGPT","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}}]}},{"title":"Musculoskeletal and connective tissue disorders","event_list":{"event":{"sub_title":"Pain: Joint","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}}}},{"title":"Nervous system disorders","event_list":{"event":[{"sub_title":"Apnea","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Ataxia (incoordination)","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Confusion","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Dizziness","counts":{"@group_id":"E1","@subjects_affected":"2","@subjects_at_risk":"9"}},{"sub_title":"Neuropathy: cranial: CN VIII Hearing and balance","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Neuropathy: sensory","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Seizure","counts":{"@group_id":"E1","@subjects_affected":"3","@subjects_at_risk":"9"}},{"sub_title":"Somnolence/depressed level of consciousness","counts":{"@group_id":"E1","@subjects_affected":"5","@subjects_at_risk":"9"}},{"sub_title":"Speech impairment","counts":{"@group_id":"E1","@subjects_affected":"2","@subjects_at_risk":"9"}},{"sub_title":"Syncope (fainting)","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}},{"sub_title":"Pain: Head/headache","counts":{"@group_id":"E1","@subjects_affected":"4","@subjects_at_risk":"9"}}]}},{"title":"Renal and urinary disorders","event_list":{"event":{"sub_title":"Hemorrhage, GU: Bladder","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}},{"title":"Respiratory, thoracic and mediastinal disorders","event_list":{"event":{"sub_title":"Dyspnea (shortness of breath)","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}},{"title":"Skin and subcutaneous tissue disorders","event_list":{"event":{"sub_title":"Rash: erythema multiforme","counts":{"@group_id":"E1","@subjects_affected":"1","@subjects_at_risk":"9"}}}}]}}},"certain_agreements":{"pi_employee":"Principal Investigators are NOT employed by the organization sponsoring the study.","restrictive_agreement":""},"point_of_contact":{"name_or_title":"S. R. Burzynski, MD, PhD","organization":"Burzynski Research Institute, Inc.","phone":"713-335-5664","email":"srb@burzynskiclinic.com"}}}'

class ValidateSchema(object):

    def __init__(self):
        self.schemas = []
        self.schema_errors = 0
        self.previous_doc = dict()
        self.index = 0
        self.exisiting_format = {}

        self.format = {}

    def clean_value_for_path(self, path, data):
        key = path[0]
        print key

        if isinstance(data, dict):
            if key in data:
                key = path.pop(0)
                value = data[key]

                if len(path) == 0:
                    print '               ', value

                    if not isinstance(value, dict):
                        value = {
                            "#text": value
                        }
                    
                    data[key] = value
                else:
                    cleaned_value = self.clean_value_for_path(path, value)
                    if cleaned_value is not None:
                        data[key] = cleaned_value
        elif isinstance(data, list):
            # print data
            cleaned_data = []
            for item in data:
                path_copy = path[:]
                cleaned_item = self.clean_value_for_path(path_copy, item)
                cleaned_data.append(cleaned_item)
                            
            data = cleaned_data

        return data

    def test(self):
        doc_json = json.loads(doc_json_str)
        doc_json = self.clean_value_for_path(["clinical_results","reported_events","serious_events","category_list","category","event_list","event","sub_title"], doc_json)
        print json.dumps(doc_json)

    def create_mapping(self, data_directory, format):
        mapping = {}
        for signature in format:
            format_item = format[signature]
            parents = format_item['parents']

            mapping = self.add_mapping(parents, mapping)

        file_utils.save_file(data_directory, 'mapping.json', mapping)


    def add_mapping(self, parents, mapping):
        child_mapping = {}
        if len(parents) == 1:
            child_mapping = {
                "type": "text"
            }
            key = parents.pop()
            mapping[key] = child_mapping
            return mapping
        else: 
            key = parents.pop(0)
            # key = parents[0]
            # parents = parents[:-1]
            print key
            properties = {}
            if key in mapping:
                if 'properties' in mapping[key]:
                    properties = mapping[key]['properties']
                # properties = mapping[key]['properties']
            child_mapping = {
                "properties" : self.add_mapping(parents, properties)
            }
            mapping[key] = child_mapping
            return mapping  

    def process(self, data_directory):
        for name in os.listdir(data_directory):
            file_path = os.path.join(data_directory, name)
            if os.path.isfile(file_path) and name.endswith('.xml'):
                print 'Parsing file:', file_path
                xmltodict.parse(open(file_path), item_depth=1, item_callback=self.handle_row)


        file_utils.save_file(data_directory, 'schema.json', self.format)

        # ft = {
        #     "test": {
        #         "data": ["test"],
        #         "parents": [ "clinical_results", "outcome_list", "outcome", "analysis_list", "analysis", "param_type" ]
        #     }
        # }

        self.create_mapping(data_directory, self.format)
        

    def append_format(self, parents, data, format):
        # print data
      
        if isinstance(data, dict):
            for key in data:
                data_item = data[key]

                parents_copy = parents[:]
                parents_copy.append(key)

                self.append_format(parents_copy, data_item, format)
        elif isinstance(data, list):
            for item in data:
                self.append_format(parents, item, format)
        else:
            signature = '_'.join(parents)
            if signature not in format:
                format[signature] = {
                    "parents": parents,
                    "data": [data]
                }
            # elif data is not None:
            #     datas = format[signature]['data']
            #     if datas is None:
            #         datas = [data]
            #     else:
            #         datas = datas.append(data)
            #     format[signature]['data'] = datas
                

        # print len(format), 'keys in format'
            

    def handle_row(self, _, row):
        self.append_format([], row, self.format)

        self.index += 1
        print 'Docs processed:', self.index , 'Keys in format:', len(self.format), 'Schema errors:', self.schema_errors
        return True

    def combine_items(self, old_item, new_item):
        if isinstance(old_item, list) and isinstance(new_item, list):
            return self.combine_lists(old_item, new_item)
        elif isinstance(old_item, dict) and isinstance(new_item, dict):
            return self.combine_dictionary(old_item, new_item)
        elif isinstance(old_item, dict) and isinstance(new_item, list):
            return self.combine_lists([old_item], new_item)
        elif isinstance(old_item, list) and isinstance(new_item, dict):
            return self.combine_lists(old_item, [new_item])

    def combine_dictionary(self, old_item, new_item):
        combined = {}
        for key in old_item:
            combined[key] = old_item[key]

        for key in new_item:
            new_item_for_key = new_item[key]
            if key in combined:
                old_item_for_key = combined[key]

                combined_item_for_key = self.combine_items(old_item_for_key, new_item_for_key)
                combined[key] = combined_item_for_key
            else:
                combined[key] = new_item_for_key

        return combined  

    def combine_lists(self, old_item, new_item):
        pre_combined_list = []
        pre_combined_list.extend(old_item)
        pre_combined_list.extend(new_item)

        combined = []
        for item in pre_combined_list:
            if len(combined) == 0:
                combined.append(item)
            else:
                first_item = combined[0]
                combined_item = self.combine_items(first_item, item)
                combined[0] = combined_item

        return combined   

    def validate_json(self, old_item, new_item):
        # Determine type of old and new item

        # If either is a list, iterate and combine objects in each array, then combine objects with each other
        list_one = []
        list_two = []
        if isinstance(old_item, list):
            list_one = old_item
        else:
            list_one = [old_item]

        if isinstance(new_item, list):
            list_two = new_item
        else:
            list_two = [new_item]
        
        # If both are dict, combine them
        




    # def validate_json(self, existing_format, new_json):
    #     for key in new_json:
    #         ef_item = {}
    #         if key in existing_format:
    #             ef_item = existing_format[key]
            
    #         new_item = new_json[key]
    #         if isinstance(ef_item, dict) and isinstance(new_item, dict):
    #             ef_item = self.validate_json(ef_item, new_item)
    #         elif isinstance(new_item, list):
    #             if isinstance(ef_item, list):
    #                 if len(ef_item) > 0:
    #                     ef_item = ef_item[0]
    #                 else:
    #                     ef_item = {}
    #                 for item in new_item:
    #                     ef_item = self.validate_json(ef_item, item)
    #             elif isinstance(ef_item, dict):
    #                  for item in new_item:
    #                     ef_item = self.validate_json(ef_item, item)
    #             else:
    #                 ef_item = [new_item[0]]
                       
            
            


    #         existing_format[key] = ef_item



    # def process(self, data_directory):
    #     root_schema = xsd_to_json_schema('data_load/clinical_trials/clinical_trials_public.xsd')
    #     root_schema_obj = json.loads(root_schema)
    #     # root_schema_string = json.dumps(root_schema_obj)
    #     # root_schema_string = root_schema_string.replace('\n', '')
    #     # root_schema_string = root_schema_string.replace('\"', '"')

    #     # print root_schema_string
    #     self.schemas.append(root_schema_obj)
    #     xml_data_directory_source = XMLDataDirectorySource(data_directory, 'data_load/clinical_trials/clinical_trials_public.xsd')
    #     xml_data_directory_source.process_rows(self.process_row_method)

    # def process_row_method(self, doc, index):
    #     if 'rank' not in doc:
    #         doc['rank'] = ""
    #     if 'variable_date_struct' not in doc:
    #         doc['variable_date_struct'] = ""
    #     doc_json_string = json.dumps(doc)
    #     for schema in self.schemas:
    #         # my_schema_object = json_schema.loads(schema)
    #         # my_schema_object.full_check(doc_json_string)
    #         # if json_schema.match(doc_json_string, schema):
    #         #     match_found = True
    #         # else:
    #         #     self.schema_errors += 1
            # try:
            #     validate(doc, schema)
            # except Exception as e:
            #     print e.message
            #     print e.args
            #     print type(e)
            #     self.schema_errors += 1
    #     print 'Docs processed:', index+1, 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors
    #     return True

    # def process_row_method(self, doc, index):
    #     # try:
    #     schema_root = {
    #         "properties": {
    #             "completion_date": {
    #                 "mergeStrategy": "overwrite"
    #             },
    #             "enrollment": {
    #                 "mergeStrategy": "overwrite"
    #             },
    #             "start_date": {
    #                 "mergeStrategy": "overwrite"
    #             },
    #             "primary_completion_date": {
    #                 "mergeStrategy": "overwrite"
    #             }
    #         }
    #     }

    #     merger = Merger(schema_root)
    #     self.previous_doc = merger.merge(self.previous_doc, doc)
    #     self.normalize_array(self.previous_doc)
    #     doc_json_string = json.dumps(self.previous_doc)
    #     # print doc_json_string
    #     match_found = False
        # for schema in self.schemas:
        #     if json_schema.match(doc_json_string, schema):
        #         match_found = True
        #         break

    #     if not match_found:
    #         try:
    #             schema_for_doc = json_schema.dumps(doc_json_string)
    #             self.schemas.append(schema_for_doc)
    #         except Exception as e:
                # print e.message
                # print e.args
                # print type(e)
    #             self.schema_errors += 1
    #             print doc_json_string
    #             exit()

    #     print 'Docs processed:', index+1, 'Schemas:', len(self.schemas), 'Schema errors:', self.schema_errors

    #     # except Exception as e:
    #     #     print type(e)
    #     #     print e.args
    #     #     print e.message
    #     #     print ''
    #     #     print json.dumps(doc)
    #     #     print json.dumps(self.previous_doc)
    #     #     return False
       
    #     return True
        

    def normalize_array(self, doc):
        if isinstance(doc, list):
            keys = []
            for item in doc:
                if isinstance(item, dict):
                    for key in item:
                        if key not in keys:
                            keys.append(key)

            for item in doc:
                if isinstance(item, dict):
                    for key in keys:
                        if key not in item:
                            item[key] = None
                self.normalize_array(item)                    
        elif isinstance(doc, dict):
            for key in doc:
                item = doc[key]
                self.normalize_array(item)
            
    


validate_schema = ValidateSchema()
# validate_schema.process('/Users/robin/Desktop/AllPublicXML/NCT0000xxxx')

validate_schema.test()

