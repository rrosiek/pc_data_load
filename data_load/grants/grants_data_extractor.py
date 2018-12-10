from data_load.base.data_extractor import DataExtractor


class GrantsDataExtractor(DataExtractor):
    @staticmethod
    def extract_id(data_source_name, row):
        if 'OpportunityID' in row:
            return row['OpportunityID']

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
