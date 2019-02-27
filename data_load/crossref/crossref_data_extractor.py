from data_load.base.data_extractor import DataExtractor


class CrossrefDataExtractor(DataExtractor):
    @staticmethod
    def extract_id(data_source_name, row):
        if 'DOI' in row:
            return row['DOI']

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
