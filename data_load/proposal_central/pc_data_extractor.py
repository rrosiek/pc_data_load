from data_load.base.data_extractor import DataExtractor


class PCDataExtractor(DataExtractor):
    @staticmethod
    def extract_id(data_source_name, row):
        if 'AwardProposalID' in row:
            return row['AwardProposalID']

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
