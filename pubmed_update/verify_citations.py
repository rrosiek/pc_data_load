import os
import pubmed_load_config
from data_load.base.data_source_xml import XMLDataSource
import data_load.base.utils.file_utils as file_utils

class VerifyCitations(object):
    def __init__(self, update_file, doc_ids):
        self.update_file = update_file
        self.doc_ids = doc_ids
        self.load_config = pubmed_load_config.get_load_config()
        self.docs_with_citations = {}
        self.new_docs = {}
        self.total_ids = {}

    def process_file(self):
        file_name = os.path.basename(self.update_file)

        self.load_config.data_source_name = file_name.split('.')[0]

        print self.update_file

        data_source = XMLDataSource(self.update_file, 2)
        data_source.process_rows(self.process_row)

        print self.update_file
        print 'Docs with citations:', len(self.docs_with_citations)
        print 'New Docs:', len(self.new_docs)
        print 'Total Docs:', len(self.total_ids)

        if len(self.docs_with_citations) > 0:
            file_utils.make_directory("docs_with_citations")
            file_utils.save_file("docs_with_citations", self.load_config.data_source_name + '.json', self.docs_with_citations)

    def process_row(self, row, current_index):
        _id =  self.load_config.data_extractor.extract_id(self.load_config.data_source_name, row)
        if _id is not None:
             
            self.total_ids[_id] = 0 

            doc = self.load_config.data_extractor.extract_data(_id, self.load_config.data_source_name, row)
            citations = self.load_config.data_mapper.get_citations([doc])
            
            if _id == '29373476':
                print doc
                raw_input('Continue?')
             
            if len(citations) > 0:
               
                if _id not in self.doc_ids:
                    self.new_docs[_id] = 0

                    if len(self.total_ids) % 100 == 0:
                        print self.load_config.data_source_name, _id, len(citations)

                    if _id not in self.docs_with_citations:
                        self.docs_with_citations[_id] = []

                    self.docs_with_citations[_id].extend(citations)

        return True
