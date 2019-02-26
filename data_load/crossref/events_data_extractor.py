from data_load.base.data_extractor import DataExtractor


class EventsDataExtractor(DataExtractor):
    @staticmethod
    def extract_id(data_source_name, row):
        if 'id' in row:
            return row['id']

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
