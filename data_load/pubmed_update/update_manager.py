import os
import sys

from config import *
from data_load.base.utils.export_doc_ids import export_doc_ids

from data_load.base.utils.data_loader_utils import DataLoaderUtils
import data_load.base.utils.file_utils as file_utils
from pubmed_updater import PubmedUpdater
from find_pmid import FindPMID
from ftp_manager import FTPManager
from verify_citations import VerifyCitations
import pubmed_load_config
import email_client
import logging
import datetime

import data_load.base.utils.log_utils as log_utils
from prospective_citations import FindProspectiveCitations

DEBUG = False


# def start():
#     update_files = [
#         '/data/data_loading/pubmed2018_updates/source_files/pubmed18n0929.xml',
#         '/data/data_loading/pubmed2018_updates/source_files/pubmed18n0930.xml'
#     ]

#     pubmed_updater = PubmedUpdater(update_files)
#     pubmed_updater.run()

def create_index():
    data_loader_utils = DataLoaderUtils(SERVER, INDEX, TYPE)
    data_loader_utils.check_and_create_index('mapping.json')


def generate_update_summary(pubmed_updater, new_update_files):
    # Update stats
    total_updated_ids_per_file = pubmed_updater.get_total_updated_ids()
    new_pmids_per_file = pubmed_updater.get_new_pmids_per_file()

    update_data = {}
    for update_file in new_update_files:
        total_updated_ids = 0
        new_pmids = 0

        if update_file in total_updated_ids_per_file:
            total_updated_ids = len(total_updated_ids_per_file[update_file])
        if update_file in new_pmids_per_file:
            new_pmids = len(new_pmids_per_file[update_file])

        updated_articles = total_updated_ids - new_pmids

        update_data[update_file] = {
            'articles_processed': total_updated_ids,
            'new_articles': new_pmids,
            'updated_articles': updated_articles
        }

    return update_data

def run(auto_update=False, new_update_files=None):
    load_config = pubmed_load_config.get_load_config()
    load_config.process_count = 1

    # Check for update files in ftp url
    # Download files not downloaded yet
    # Update downloaded files list
    FTPManager(load_config).download_new_update_files()

    # Get a batch of update files
    if new_update_files == None:
        new_update_files = get_batch_update_files(load_config, 4)

    process_update_files(auto_update, load_config, new_update_files)

    # new_update_files = [
    #     '/data/data_loading/pubmed2018_updates/source_files/pubmed18n1050.xml',
    #     '/data/data_loading/pubmed2018_updates/source_files/pubmed18n1052.xml',
    #     '/data/data_loading/pubmed2018_updates/source_files/pubmed18n1054.xml',
    #     '/data/data_loading/pubmed2018_updates/source_files/pubmed18n1056.xml',
    #     '/data/data_loading/pubmed2018_updates/source_files/pubmed18n1059.xml'
    # ]

def get_batch_update_files(load_config, no_of_files_in_batch):
    total_new_update_files = get_new_update_files(load_config)

    new_update_files = []
    max_length = min(len(total_new_update_files), no_of_files_in_batch)
    # print max_length, 'max_length'
    for i in range(0, max_length):
        new_update_file = total_new_update_files[i]
        new_update_files.append(new_update_file)
    
    return new_update_files

def process_update_files(is_auto_update, load_config, new_update_files):
    now = datetime.datetime.now()
    local_date = now.strftime("%m-%d-%Y")

    logger = log_utils.create_logger('pubmed2018_update', load_config.log_files_directory())
    logger.info(str(len(new_update_files)) +  ' update files to process')

    if len(new_update_files) > 0:
        logger.info(str(new_update_files))

        # Send update start notification
        if is_auto_update:
            email_client.send_update_start_notification(local_date, new_update_files)

        # Process files not processed yet
        # Update processed files list
        pubmed_updater = PubmedUpdater(logger, new_update_files)
        pubmed_updater.run()

        # Send prospective cites notifications
        docs_with_new_citations = pubmed_updater.get_docs_with_new_citations()

        # Get the update summary
        logger.info('Generating update summary...')
        update_data = generate_update_summary(pubmed_updater, new_update_files)

        logger.info('Saving update summary...')
        save_update_record_for_date(load_config, local_date, update_data, docs_with_new_citations)
        
        # all_prospects = send_prospective_citations_notifications(logger, docs_with_new_citations)

        # # Send update notification
        # logger.info('Sending update status mail...')
        # email_client.send_update_notifications(local_date, update_data, all_prospects)

        # Save existing pmids to file
        logger.info('Saving new pmids...')
        pubmed_updater.save_new_pmids()

        # Update processed files list
        update_processed_update_files(new_update_files)
    else:
        if is_auto_update:
            # Send update notification
            logger.info('Sending update status mail...')
            email_client.send_update_notifications(local_date, [], [])


### Update records

def get_update_records_directory(load_config):
    other_files_directory = load_config.other_files_directory()
    update_records_directory = other_files_directory + '/' + 'update_records'
    file_utils.make_directory(update_records_directory)
    return update_records_directory


def save_update_record_for_date(load_config, local_date, update_data, docs_with_new_citations):
    update_records_directory = get_update_records_directory(load_config)
    local_time = now.strftime("%H:%M:%S")

    update_record = {
        'local_date': local_date,
        'update_data': update_data,
        'docs_with_new_citations': docs_with_new_citations,
    }
    file_utils.save_file(update_records_directory, 'pubmed_update_record_' + local_date + '_' + local_time, update_record)
 

def get_update_records(load_config):
    update_records_directory = get_update_records_directory(load_config)
    update_records = []

    for name in os.listdir(update_records_directory):
        file_path = os.path.join(update_records_directory, name)
        if os.path.isfile(file_path) and name.startswith("pubmed_update_record_"):
            update_records.append(name)

    update_records.sort()

    return update_records


def process_update_record(load_config, update_record_name):
    logger = log_utils.create_logger('pubmed2018_update', load_config.log_files_directory())
    logger.info('Loading update record: ' + str(update_record_name))

    update_records_directory = get_update_records_directory(load_config)
    update_record = file_utils.load_file(update_records_directory, update_record_name)

    local_date = update_record['local_date']
    update_data = update_record['update_data']
    docs_with_new_citations = update_record['docs_with_new_citations']

    logger.info('Update record loaded')
    logger.info('Date: ' + str(local_date))
    logger.info('Update files: ')
    for update_file in update_data:
        logger.info(update_file)

    logger.info('Docs with new citations: ' + str(len(docs_with_new_citations)))

    all_prospects = send_prospective_citations_notifications(logger, docs_with_new_citations)

    # Send update notification
    logger.info('Sending update status mail...')
    email_client.send_update_notifications(local_date, update_data, all_prospects)

    logger.info('Done')


def retry_prospective_citations():
    load_config = pubmed_load_config.get_load_config()
    load_config.process_count = 1

    update_records = get_update_records(load_config)

    if len(update_records) == 0:
        print('0 update records, exiting')
        return

    print('Update records')
    print('--------------')
    index = 0
    for update_record in update_records:
        index += 1
        print(str(index) + ': ' + update_record)
    
    record_index = raw_input('Choose a record to retry' + '(' + str(1) + '-' + str(index) + ') ')
    try:
        record_index = int(record_index)
        if record_index >= 1 and record_index <= index:
            update_record = update_records[record_index - 1]
            process = raw_input('Process ' + str(update_record) + '? (y/n)')
            if process.lower() in ['y', 'yes']:
                process_update_record(load_config, update_record)
        else:
            print('Wrong index, try again')
    except Exception as e:
        print(str(e))

def send_prospective_citations_notifications(logger, docs_with_new_citations):
    # Find prospective cites and send email notifications to subsbribed users
    find_prospective_citations = FindProspectiveCitations(logger, docs_with_new_citations)
    return find_prospective_citations.run()

def get_new_update_files(load_config):
    processed_update_files = get_processed_update_files(load_config)
    new_update_files = []

    source_files = get_all_update_files(load_config)
    for file_path in source_files:
        if file_path not in processed_update_files:
            new_update_files.append(file_path)

    return new_update_files


def get_all_update_files(load_config):
    source_files = []
    source_files_directory = load_config.source_files_directory()
    for name in os.listdir(source_files_directory):
        source_files.append(name)

    source_files.sort()
    source_file_paths = []
    for name in source_files:
        file_path = os.path.join(source_files_directory, name)
        if os.path.isfile(file_path) and name.endswith('.xml'):
            source_file_paths.append(file_path)

    return source_file_paths


def get_processed_update_files(load_config):
    other_files_directory = load_config.other_files_directory()
    processed_file_urls = file_utils.load_file(
        other_files_directory, PROCESSED_UPDATE_FILES)
    if len(processed_file_urls) == 0:
        return []

    return processed_file_urls


def set_processed_update_files(load_config, processed_file_urls):
    other_files_directory = load_config.other_files_directory()
    file_utils.save_file(other_files_directory,
                            PROCESSED_UPDATE_FILES, processed_file_urls)


def update_processed_update_files(update_files):
    # Update processed update files
    load_config = pubmed_load_config.get_load_config()
    processed_update_files = self.get_processed_update_files(load_config)
    processed_update_files.extend(update_files)
    set_processed_update_files(load_config, processed_update_files)

def verify_citations():
    load_config = pubmed_load_config.get_load_config()
    load_config.process_count = 1

    FTPManager(load_config).download_new_update_files()

    print 'Loading pubmed ids...'

    doc_ids = file_utils.load_file(
        load_config.index, load_config.index + '_ids.json')

    if len(doc_ids) == 0:
        doc_ids = export_doc_ids(load_config.server, load_config.index,
                                 load_config.type, load_config.index, load_config.index + '_ids.json')

    print len(doc_ids), 'Total pubmed ids'

    total_new_update_files = get_all_update_files(load_config)

    filtered_update_files = []
    for update_file in total_new_update_files:
        if '1010.xml' in update_file:
            filtered_update_files.append(update_file)

    print 'Total update files:', len(filtered_update_files)
    print filtered_update_files
    if len(filtered_update_files) > 0:
        for new_update_file in filtered_update_files:
            print 'Processing file:', new_update_file
            verify_citations = VerifyCitations(new_update_file, doc_ids)
            verify_citations.process_file()
        #    raw_input('Continue?')

# verify_citations()

# def send_mail():
#     failed_prospects = []
#     all_prospects = file_utils.load_file('data_load/pubmed_update', 'prospects1.json')

#     # Send email notifications
#     for prospect in all_prospects:
#         problems = email_client.send_notification_for_prospect(prospect)
#         if len(problems) > 0:
#             failed_prospects.append({
#                 'problems': problems,
#                 'prospect': prospect
#             })

def find_pmid(pmid, update_file=None):

    if update_file is None:
        load_config = pubmed_load_config.get_load_config()
        load_config.process_count = 1
        update_files = get_all_update_files(load_config)
        for update_file in update_files:
            find_pmid(pmid, update_file)
    else:
        file_name = os.path.basename(update_file)

        load_config = pubmed_load_config.get_load_config()
        load_config.data_source_name = file_name.split('.')[0]
    
        find_pmid_obj = FindPMID(load_config, [pmid], update_file)
        find_pmid_obj.run()


if __name__ == "__main__":

    if len(sys.argv) >= 2:
        param_type = sys.argv[1]
        if param_type == '-auto':
            run(auto_update=True)
        elif param_type == '-f':
            if len(sys.argv) == 3:
                name = sys.argv[2]
                run(auto_update=False, new_update_files=[name])
            else:
                print 'Enter update file path (update_manager -f <file_path>)'
        elif param_type == '-d':
            if len(sys.argv) == 3:
                pmid = sys.argv[2]
                find_pmid(pmid)
            elif len(sys.argv) == 4:
                pmid = sys.argv[2]
                update_file = sys.argv[3]
                find_pmid(pmid, update_file)
            else:
                print 'Usage: (update_manager -d <pmid>)'
                print 'Usage: (update_manager -d <pmid> <file_path>)'
        elif param_type == '-retry':
            retry_prospective_citations()
        else:
            print 'Invalid parameter'
    else:
        run()

    # send_mail()

