import csv
import os
import sys
import codecs


from data_source import DataSource
from utils.logger import *

class CSVDataSource(DataSource):

    def __init__(self, data_source_file_path):
        super(CSVDataSource, self).__init__(data_source_file_path)

        csv.field_size_limit(sys.maxsize)


    # def get_unique_ids(self):
    #     count = 0
    #     unique_ids = {}
    #
    #     self.open_data_file()
    #     if self.data_source_file is not None:
    #         reader = csv.DictReader(self.data_source_file)
    #         for row in reader:
    #             _id = self.extract_id(self.data_source_name, row)
    #
    #             if _id is not None:
    #                 if _id not in unique_ids:
    #                     unique_ids[_id] = 1
    #                 else:
    #                     unique_ids[_id] += 1
    #
    #             count += 1
    #             # if count % 25000 == 0:
    #             #     print 'Sample row', row
    #             if count % 1000000 == 0:
    #                 print 'Processed rows', count, len(unique_ids)
    #
    #     print 'Total rows to process', count
    #     print 'Total ids to process', len(unique_ids)
    #
    #     return unique_ids

    def process_rows(self, process_row_method):
        super(CSVDataSource, self).process_rows(process_row_method)
        self.logger.log(LOG_LEVEL_TRACE, 'Data source reading rows...')

        self.current_index = 0
        self.open_data_file()
        if self.data_source_file is not None:
            reader = csv.DictReader(self.data_source_file)
            for row in reader:
                self.current_index += 1
                if not self.process_row_method(row, self.current_index):
                    break
        else:
            self.logger.log(LOG_LEVEL_ERROR, 'Data source file is null', self.data_source_file_path)

        self.close_data_file()

    def clean_file(self, data_source_file_path):
        file_name = os.path.basename(data_source_file_path)
        directory = os.path.dirname(data_source_file_path)
        output_file_name = 'cleaned_' + file_name
        output_file_path = os.path.join(directory, output_file_name)
        try:
            output_file = open(output_file_path, 'r')
            self.logger.log(LOG_LEVEL_INFO, output_file_path, 'exists, skipping clean...')
            return output_file_path
        except Exception as e:
            self.logger.log(LOG_LEVEL_WARNING, e.message)
            self.logger.log(LOG_LEVEL_INFO, output_file_path, 'does not exist, cleaning...')

        output_file = open(output_file_path, 'w')

        with open(data_source_file_path, "r") as ins:
            line_count = 0
            for line in ins:
                if line is not None and len(line) > 0:
                    line_count += 1
                    line_comps = line.split('\x00')
                    clean_line = ' '.join(line_comps)
                    if line_count % 500000 == 0:
                        self.logger.log(LOG_LEVEL_INFO, 'Processing line:', str(line_count))
                    output_file.write(clean_line + '\n')

        output_file.close()
        return output_file_path
