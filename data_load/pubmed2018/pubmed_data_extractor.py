from data_load.base.data_extractor import DataExtractor


class PubmedDataExtractor(DataExtractor):

    @staticmethod
    def extract_id(data_source_name, row):
        if 'MedlineCitation' in row:
            if 'PMID' in row['MedlineCitation']:
                pmid_dict = row['MedlineCitation']['PMID']
                if isinstance(pmid_dict, dict):
                    return pmid_dict['content']
                else:
                    return pmid_dict

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
