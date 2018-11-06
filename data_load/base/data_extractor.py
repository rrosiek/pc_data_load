
class DataExtractor(object):

    @staticmethod
    def should_generate_id(data_source_name):
        return False

    @staticmethod
    def generate_id(count):
        return str(count)

    @staticmethod
    def extract_id(data_source_name, row):
        return None

    @staticmethod
    def extract_data(_id, data_source_name, row):

        return None

    @staticmethod
    def get_value_for_field(field, row, condition=None):
        condition_result = True
        if condition:
            condition_result = DataExtractor.evaluate_condition(row, condition)

        field_value = ''
        if condition_result:
            if field in row:
                field_value = row[field]

        return field_value

    @staticmethod
    def evaluate_condition(row, condition):
        condition_result = False
        if condition:
            condition_field = condition['field']
            condition_value = condition['value']

            if condition_field in row:
                c_value = row[condition_field]

                if condition_value == c_value:
                    condition_result = True
                else:
                    condition_result = False

        return condition_result
