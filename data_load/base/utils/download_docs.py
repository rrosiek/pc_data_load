import data_load.base.utils.file_utils as file_utils

import data_load.base.utils.export_doc_ids as export_doc_ids
from data_load.base.utils.data_utils import DataUtils
import data_load.base.utils.file_utils as file_utils

class DownloadDocs(object):

    def __init__(self, server, src_index, src_type, ids=None):
        self.server = server
        self.index = src_index
        self.type = src_type
        self.ids = ids
        self.docs = []

        self.data_utils = DataUtils

    def docs_fetched(self, docs, index, type):
        self.docs.extend(docs)

    def save_docs(directory, file_name):
        docs = self.run()
        file_utils.pickle_file(directory, file_name, docs)

    def run(self):
        if self.ids == None:
            self.ids = export_doc_ids.export_doc_ids(server=self.server, 
                                                        src_index=self.index, 
                                                        src_type=self.type)
                                                    

            self.ids = self.ids.keys()

        print len(self.ids), 'ids'

        self.data_utils.batch_fetch_docs_for_ids(base_url=self.server, 
                                            ids=self.ids, 
                                            index=self.index, 
                                            type=self.type, 
                                            docs_fetched=self.docs_fetched)

        print len(self.docs), 'docs fetched'
        return self.docs