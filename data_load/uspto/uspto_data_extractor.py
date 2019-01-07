from data_load.base.data_extractor import DataExtractor


class USPTODataExtractor(DataExtractor):
    @staticmethod
    def extract_id(data_source_name, row):
        if 'us-patent-grant' in row:
            if 'us-bibliographic-data-grant' in row['us-patent-grant']:
                if 'publication-reference' in row['us-patent-grant']['us-bibliographic-data-grant']:
                    if 'document-id' in row['us-patent-grant']['us-bibliographic-data-grant']['publication-reference']:
                        if 'doc-number' in row['us-patent-grant']['us-bibliographic-data-grant']['publication-reference']['document-id']:
                            doc_number = row['us-patent-grant']['us-bibliographic-data-grant']['publication-reference']['document-id']['doc-number']
                            return doc_number

        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):
        return row
