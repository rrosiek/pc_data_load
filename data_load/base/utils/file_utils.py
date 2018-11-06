import json
import csv
import os
import time
import pickle
from random import randint


def save_file(directory, file_name, data):
    # print 'Saving file', file_name
    data_file = open(directory + '/' + file_name, 'w')
    data_file.write(json.dumps(data))
    data_file.close()


def save_text_file(directory, file_name, text_data):
    data_file = open(directory + '/' + file_name, 'w')
    data_file.write(text_data)
    data_file.close()


def load_text_file(directory, file_name):
    text_data = ''
    try:
        file_path = file_name
        if len(directory) > 0:
            file_path = directory + '/' + file_path
        data_file = open(file_path, 'r')
        text_data = data_file.read()
        data_file.close()
    except Exception as e:
        pass

    return text_data


def load_file(directory, file_name):
    data = {}
    try:
        file_path = file_name
        if len(directory) > 0:
            file_path = directory + '/' + file_path
        data_file = open(file_path, 'r')
        data = json.load(data_file)
        data_file.close()
    except Exception as e:
        print('LOAD FILE ERROR: ' + directory + ' ' + file_name)
        pass

    return data


def load_file_path(file_path):
    data = {}
    try:
        data_file = open(file_path, 'r')
        data = json.load(data_file)
        data_file.close()
    except Exception as e:
        # print 'error:', e.message
        pass

    return data


def make_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_file_csv(directory, file_name, data):
    print('Saving file ' + file_name)
    fieldnames = ['k', 'v']

    file_path = file_name
    if len(directory) > 0:
        file_path = directory + '/' + file_path
    data_file = open(file_path, 'w')
    writer = csv.DictWriter(data_file, fieldnames=fieldnames)
    writer.writeheader()
    for key in data:
        value = json.dumps(data[key])
        writer.writerow({
            'k': key,
            'v': value
        })

    data_file.close()


def load_file_csv(directory, file_name):
    data = {}
    try:
        file_path = file_name
        if len(directory) > 0:
            file_path = directory + '/' + file_path
        data_file = open(file_path, 'r')
        reader = csv.DictReader(data_file)
        for row in reader:
            key = row['k']
            value = json.loads(row['v'])
            data[key] = value

        data_file.close()
    except Exception as e:
        # print 'error:', e.message
        pass

    return data


def pickle_file(directory, file_name, data):
    print('Pickling file ' + file_name)
    file_path = file_name
    if len(directory) > 0:
        file_path = directory + '/' + file_path
    f = open(file_path, 'wb')
    pickle.dump(data, f)
    f.close()
    print('Done pickling file ' + file_name)


def unpickle_file(directory, file_name):
    print('Unpickling file ' + file_name)

    data = {}
    try:
        file_path = file_name
        if len(directory) > 0:
            file_path = directory + '/' + file_path
        f = open(file_path, 'rb')
        data = pickle.load(f)
        f.close()
    except Exception as e:
        # print 'error:', e.message
        pass

    return data


def batch_file_name_with_prefix(prefix):
    batch_id = str(int(round(time.time() * 1000))) + '_' + str(randint(100, 999))
    batch_file_name = prefix + '_' + batch_id
    return batch_file_name
