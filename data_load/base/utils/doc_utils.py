from collections import OrderedDict
import json




#############################################################################
# Utils
#############################################################################

def extract_title_for_doc_and_index(doc, field_paths):
    title = get_value_string_for_field_from_doc_for_index(doc, field_paths)
    return title


#############################################################################

def get_string_representation(values, separator):
    value_string = ''

    if type(values) is list:
        for value in values:
            if len(value_string) > 0:
                value_string += separator
            value_string += get_string_representation(value, ' ')
    else:
        value_string = values

    try:
        value_string = str(value_string)
        value_string = value_string.encode('utf-8')
        value_string = value_string.strip()
    except Exception as e:
        pass
        # print(e.message)

    return value_string

def get_value_string_for_field_from_doc_for_index(doc, field_paths):
    return get_value_string_for_field_paths_from_doc_for_index(field_paths, doc)


def get_value_string_for_field_paths_from_doc_for_index(field_paths, doc):
    if '_id' in field_paths:
        values = get_values_from_doc(doc, field_paths)
    else:
        values = get_values_from_doc_source(doc, field_paths)

    value = get_string_representation(values, ', ')
    return value


def get_values_from_doc(doc, paths):
    values = []
    for path in paths:
        path_comps = path.split('.')
        value = doc
        for path_comp in path_comps:
            value = value[path_comp]
            values.append(str(value))

    return values


def generate_path_tree(paths):
    split_paths = []
    max_length = 0
    for path in paths:
        split_path = path.split('.')
        split_paths.append(split_path)
        if len(split_paths) > max_length:
            max_length = len(split_path)

    path_tree = OrderedDict()
    for split_path in split_paths:
        temp_path_tree = path_tree
        for path_item in split_path:
            if path_item not in temp_path_tree:
                temp_path_tree[path_item] = OrderedDict()
            temp_path_tree = temp_path_tree[path_item]

    # print(path_tree)

    return path_tree


def traverse_path_tree(path_tree, value):
    # print('***********************')
    # print(path_tree)
    # print(value)
    values = []
    for path in path_tree:
        sub_path = path_tree[path]
        try:
            value = json.loads(value)
        except Exception as e:
            pass

        if value and path in value:
            sub_value = value[path]

            sub_values = []
            if type(sub_value) is list:
                for item in sub_value:
                    sub_values.append(item)
            else:
                sub_values.append(sub_value)

            if not sub_path:
                # print(sub_values)
                values.extend(sub_values)
            else:
                for item in sub_values:
                    values.append(traverse_path_tree(sub_path, item))

    if len(values) == 1:
        return values[0]
    return values


def get_values_from_doc_source(doc, paths):
    if '_source' in doc:
        doc_source = doc['_source']
        path_tree = generate_path_tree(paths)
        return traverse_path_tree(path_tree, doc_source)
    return []


def get_field_from_doc_source(doc, path):
    values = []

    path_comps = path.split('.')
    doc_source = doc['_source']
    value = doc_source
    try:
        for path_comp in path_comps:
            if type(value) is list:
                item_values = []
                for item in value:
                    item_value = item[path_comp]
                    if item_value is not None:
                        item_values.append(str(item_value))
                value = item_values
            else:
                value = value[path_comp]

        if type(value) is list:
            values.extend(value)
        elif value is not None:
            values.append(value)
    except Exception as e:
        pass
        # print (e.message)

    return values


def extract_content(value):
    try:
        value = json.loads(value)
    except Exception as e:
        # print (e.message)
        pass

    values = []
    if type(value) is list:
        for v in value:
            values.extend(extract_content(v))
    elif isinstance(value, dict):
        if 'content' in value:
            content = value['content']
            values.extend(extract_content(content))
        else:
            for k in value:
                k_value = value[k]
                values.extend(extract_content(k_value))
    else:
        values.append(str(value))

    return values
