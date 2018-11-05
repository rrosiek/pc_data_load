import json
import os

from database import pardi_database
import file_utils


class CSVDownloader:
    def __init__(self, directory):
        self.directory = directory

    def csv_directory(self):
        directory = self.directory
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def csv_file_path(self, table_name):
        return self.csv_directory() + '/' + table_name + '.csv'

    def download(self, table_name):
        try:
            query = """select * from """ + table_name
            print 'Downloading', table_name, query
            file_path = self.csv_file_path(table_name)
            CSVDownloader.execute_and_save(query, None, pardi_database, file_path)
            return file_path
        except Exception as e:
            print e.message
            return None

    @staticmethod
    def execute_and_save(query, params, db, file_name):
        cursor = db.cursor()
        if params is not None:
            outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query % params)
        else:
            outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)

        with open(file_name, 'w') as f:
            cursor.copy_expert(outputquery, f)

    @staticmethod
    def execute_query_fetch_one(query, params, db):
        with db.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
        return result

    @staticmethod
    def execute_query_fetch_all(query, params, db):
        with db.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result









