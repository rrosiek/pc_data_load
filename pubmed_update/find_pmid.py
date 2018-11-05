from data_load.base.data_source_xml import XMLDataSource


class FindPMID(object):

    def __init__(self, load_config, pmids, update_file):
        self.load_config = load_config
        self.update_file = update_file
        self.pmids = pmids

    def run(self):
        print 'Searching', self.update_file, 'for pmids', self.pmids
        data_source = XMLDataSource(self.update_file, 2)
        data_source.process_rows(self.count_row)

    def count_row(self, row, current_index):
        # if current_index % 10000 == 0:
        #     print 'Searching', self.update_file, 'for pmids', self.pmids, ',', current_index, 'ids processed'
        _id = self.extract_id(
            self.load_config.data_source_name, row, current_index)
        if _id is not None:
            if _id in self.pmids:
                print 'Found', _id, 'in', self.update_file
                return True

        return True

    def extract_id(self, name, row, current_index):
        if self.load_config.data_extractor is not None:
            if self.load_config.data_extractor.should_generate_id(name):
                return self.load_config.data_extractor.generate_id(current_index)
            else:
                return self.load_config.data_extractor.extract_id(name, row)

        self.load_config.log(
            LOG_LEVEL_WARNING, 'Error: no data extractor configured')
        return None