from data_load.clinical_trials.ct_data_extractor import CTDataExtractor
from data_load.clinical_trials.ct_data_mapper import CTDataMapper
from data_load.base.load_manager import LoadManager

# from data_load.base.utils.download_data import CSVDownloader
from data_load.base.utils.data_loader_utils import DataLoaderUtils
from data_load.base.data_source_processor import DataSourceProcessor
from data_load.base.data_source_xml_2 import XMLDataDirectorySource
from data_load.clinical_trials.ct_references_relationship_processor import CTReferencesRelationshipProcessor
from data_load.clinical_trials.ct_publications_relationship_processor import CTPublicationsRelationshipProcessor

import data_load.clinical_trials.clear_relations as clear_pubmed_relations

from data_load.base.constants import *
import data_load.base.utils.es_utils as es_utils

from config import *
import sys
import psutil
from data_load.base.utils.log_utils import *

class CTLoadManager(LoadManager):

    def __init__(self, data_directory=None):
        super(CTLoadManager, self).__init__(ID_CLINICAL_TRIALS)
        # self.data_directory = data_directory
        if data_directory is not None:
            self.data_directories = [data_directory]
        else:
            self.data_directories = []

    # Methods to override
    def should_reload(self):
        return False

    def get_info_for_index_id(self, index_id):
        return {
            "index": "clinical_trials",
            "index_type": "study"
        }

    def mapping_file_path(self):
        return 'data_load/clinical_trials/mapping.json'

    def get_data_mapper(self):
        return CTDataMapper()

    def get_data_extractor(self):
        return CTDataExtractor()

    def get_max_memory_percent(self):
        return 75

    def get_tasks_list(self):
        tasks_list = []

        for data_directory in self.data_directories:
            tasks_list.append({
                'name': data_directory,
                'status': ''
            })

        # for data_source in DATA_SOURCES:
        #     tasks_list.append({
        #         'name': data_source,
        #         'status': ''
        #     })
            
        # tasks_list.append({
        #         'name': 'ct_references_relations',
        #         'status': ''
        #     })
        
        # tasks_list.append({
        #         'name': 'ct_publications_relations',
        #         'status': ''
        #     })
            
        return tasks_list

    def run_task(self, task):
        # if task == 'process_data_directory':
        self.process_data_directory(task)
        # if task == 'copy_tags_and_annotations':
        #     self.copy_tags_and_annotations()
        # if task == 'clear_pubmed_relationships':
        #     print 'Clear relations'
        #     self.clear_pubmed_relationships()
        # if task == 'ct_references_relations':
        #     print 'processing', task
        #     self.process_ct_references_relationships()
        # if task == 'ct_publications_relations':
        #     print 'processing', task
        #     self.process_ct_publications_relationships()
 
    def clear_pubmed_relationships(self):
        proceed = raw_input('Reload relationships from PubMed to Clinical Trials? (y/n): ')
        if proceed.lower() in ['y', 'yes', '']:
            index_item = es_utils.get_info_for_index_id(ID_PUBMED)
            pubmed_index = index_item['index']
            pubmed_type = index_item['index_type']

            clear_pubmed_relations.run(self.get_load_config(), pubmed_index, pubmed_type)
        else:
            exit()
            
    def download_data(self):
        all_public_xml_url = 'https://clinicaltrials.gov/AllPublicXML.zip'
        load_config = self.get_load_config()

        source_files_directory = self.load_config.source_files_directory()

        file_name = os.path.basename(all_public_xml_url)
        file_path = os.path.join(source_files_directory, file_name)

        # Download update zip file
        urllib.urlcleanup()
        print 'Downloading file: ', all_public_xml_url
        urllib.urlretrieve(all_public_xml_url, file_path)
        print 'Saved', file_path

        # TODO - Verify download with md5?

        # Extract update zip file

        print 'Unzipping file', file_path
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(source_files_directory)


            for name in os.listdir(source_files_directory):
                dir_path = os.path.join(source_files_directory, name)
                if os.path.isdir(dir_path):
                    for sub_dir_name in os.listdir(dir_path) and sub_dir_name.startswith('NCT'):
                        sub_dir_path = os.path.join(dir_path, sub_dir_name)

                        self.data_directories.append(sub_dir_path)

        except Exception as e:
            print e

    def process_data_directory(self, data_directory):
        print 'processing', data_directory

        load_config = self.get_load_config()
        load_config.data_source_name = 'clinical_study'
        load_config.log_level = LOG_LEVEL_DEBUG
 

        # source_files_directory = load_config.source_files_directory()
        # data_source_file_path = source_files_directory + '/' + data_source_name + '.csv'

        # print 'Processing file', data_source_file_path

        xml_data_directory_source = XMLDataDirectorySource(data_directory, 'data_load/clinical_trials/clinical_trials_public.xsd')
        data_processor = DataSourceProcessor(load_config, xml_data_directory_source)
        data_processor.run()

    def process_ct_references_relationships(self):
        print 'processing', 'ct_references'
        load_config = self.get_load_config()
        load_config.data_source_name = "ct_references"

        # Relationships
        load_config.append_relations = False

        source_files_directory = load_config.source_files_directory()
        data_source_file_path = source_files_directory + '/' + 'ct_references.csv'

        data_processor = CTReferencesRelationshipProcessor(load_config, CSVDataSource(data_source_file_path))
        data_processor.run()

    def process_ct_publications_relationships(self):
        print 'processing', 'ct_publications_relations'
        load_config = self.get_load_config()
        load_config.data_source_name = "ct_publications_relations"

        data_processor = CTPublicationsRelationshipProcessor(load_config)
        data_processor.run()

def start(data_directory):
    load_manager = CTLoadManager(data_directory)
    load_manager.del_config()
    load_manager.run()

def download_and_start():
    load_manager = CTLoadManager()
    load_manager.del_config()
    load_manager.run()

def resume():
    load_manager = CTLoadManager()
    load_manager.run()

def run():
    arg_index = 0
    for arg in sys.argv:
        if arg_index > 0:
            if arg == '-path':
                if (arg_index + 1) < len(sys.argv):
                    data_directory = sys.argv[arg_index + 1]
                    print 'Data directory', data_directory
                    start(data_directory)
                    return
                else:
                    print('Usage: ct_load_manager -path <data_directory>')    
            elif arg == '-auto':
                download_and_start()
                return
            else: 
                print('Usage: ct_load_manager -path <data_directory>')     
        arg_index += 1

    resume()

# run()
for i in range(0, 373):
    count_str = str(i)
    while len(count_str) < 4:
         count_str = '0' + count_str

    path = '/Users/robin/Desktop/AllPublicXML/NCT' + count_str + 'xxxx'
    
    start(path)