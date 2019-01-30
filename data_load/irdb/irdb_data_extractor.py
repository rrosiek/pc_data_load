from data_load.base.data_extractor import DataExtractor
from config import *
import time

class IRDBDataExtractor(DataExtractor):

    @staticmethod
    def clean_row(row):
        cleaned_row = {}
        for key in row:
            value = row[key]
            key = key.replace("\xef\xbb\xbf", "")
            cleaned_row[key] = value

        return cleaned_row

    @staticmethod
    def extract_id(data_source_name, row):
        row = IRDBDataExtractor.clean_row(row)
        # print row
        # time.sleep(5)

        _id = None
        
        if ID_FIELD in row:
            _id = row[ID_FIELD]
        elif ID_FIELD.upper() in row:
            _id = row[ID_FIELD.upper()]
            
        return _id

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
