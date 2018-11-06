from data_load import DATA_LOAD_CONFIG


RELATIONSHIP_TYPE_CITATIONS = 'citations'
RELATIONSHIP_TYPE_CITED_BYS = 'cited_bys'
RELATIONSHIP_TYPE_RELATIONS = 'relations'

DATA_SOURCE_BATCH_PREFIX = 'data_source_batch'
DATA_LOADER_BATCH_PREFIX = 'data_loader_batch'

ID_PUBMED = 'PUBMED'
ID_USPTO = 'USPTO'
ID_CLINICAL_TRIALS = 'CLINICAL_TRIALS'
ID_PROPOSAL_CENTRAL = 'PROPOSAL_CENTRAL'
ID_CLINICAL_GUIDELINES = 'CLINICAL_GUIDELINES'
ID_FDA_PURPLE_BOOK = 'FDA_PURPLE_BOOK'
ID_FDA_PATENTS = 'FDA_PATENTS'
ID_FDA_PRODUCTS = 'FDA_PRODUCTS'
ID_GRANTS = 'GRANTS'
ID_UBER_DIMENSIONS = 'UBER_DIMENSIONS'
ID_DERWENT_PATENTS = 'DERWENT_PATENTS'
ID_WEB_OF_SCIENCE = 'WEB_OF_SCIENCE'
ID_IRDB = 'IRDB'
ID_DWPI = 'DWPI'

DATA_SOURCES = [
    ID_PUBMED,
    ID_USPTO,
    ID_CLINICAL_TRIALS,
    ID_CLINICAL_GUIDELINES,
    ID_FDA_PURPLE_BOOK,
    ID_FDA_PRODUCTS,
    ID_FDA_PATENTS,
    ID_GRANTS,
    ID_UBER_DIMENSIONS,
    ID_DERWENT_PATENTS,
    ID_WEB_OF_SCIENCE,
    ID_IRDB,
    ID_DWPI,
    ID_PROPOSAL_CENTRAL
]


# LOAD SETTINGS
KEY_LOAD_TYPE = 'load_type'
KEY_SOURCE_FILES_DIRECTORY = 'source_files_directory'
KEY_SHOULD_DOWNLOAD_FILES = 'should_download_files'

KEY_ES_INDEX_ID = 'es_index_id'
KEY_ES_INDEX = 'es_index'
KEY_ES_TYPE = 'es_type'
KEY_ES_ID_PREFIX = 'es_id_prefix'

KEY_ES_MAPPING_FILE_PATH = 'es_mapping_file_path'
KEY_ES_SERVER_URL = 'es_server_url'


OPTION_LOAD_TYPE_CREATE = 'CREATE'
OPTION_LOAD_TYPE_UPDATE = 'UPDATE'

# Server
# SERVER = 'http://52.202.35.240:9200'
# LOCAL_SERVER = 'http://localhost:9200'
LOCAL_SERVER = DATA_LOAD_CONFIG.ELASTICSEARCH_SERVER

# API_URL = "https://ocat-dev.altum.com/api/v1/"
API_URL = DATA_LOAD_CONFIG.API_URL

# Batch Size & Process Count
DATA_LOADER_BATCH_SIZE = 5000
DATA_SOURCE_BATCH_SIZE = 10000000

DOC_FETCH_BATCH_SIZE = 250

PROCESS_COUNT = 2
PROCESS_SPAWN_DELAY = 0.25  # seconds

BULK_DATA_SIZE = 1500000

MAX_RETRIES = 3

# Directories
SOURCE_FILES_DIRECTORY = 'source_files'
GENERATED_FILES_DIRECTORY = 'generated_files'
OTHER_FILES_DIRECTORY = 'temp_files'
LOG_FILES_DIRECTORY = 'logs'

FAILED_DOCS_DIRECTORY = 'failed_docs'
LOADED_DOCS_DIRECTORY  = 'loaded_docs'
BULK_UPDATE_RESPONSE_DIRECTORY  = 'bulk_update_response'

DATA_SOURCE_BATCHES_FILE = 'data_source_batches.json'


DATA_LOADING_DIRECTORY = '/data/data_loading/'


INDEX_MAPPING = {}

INDEX_MAPPING[ID_PUBMED] = {
    'index': 'pubmed2018_v5',
    'type': 'article'
}
INDEX_MAPPING[ID_IRDB] = {
    'index': 'irdb_v3',
    'type': 'grant'
}
INDEX_MAPPING[ID_WEB_OF_SCIENCE] = {
    'index': 'wos_v2',
    'type': 'article'
}
INDEX_MAPPING[ID_USPTO] = {
    'index': 'uspto_v5',
    'type': 'grant'
}
INDEX_MAPPING[ID_CLINICAL_TRIALS] = {
    'index': 'clinical_trials_v2',
    'type': 'study'
}
INDEX_MAPPING[ID_CLINICAL_GUIDELINES] = {
    'index': 'clinical_guidelines_v3',
    'type': 'record'
}
INDEX_MAPPING[ID_FDA_PURPLE_BOOK] = {
    'index': 'fda_purple_book',
    'type': 'page'
}
INDEX_MAPPING[ID_FDA_PATENTS] = {
    'index': 'fda_patents_v3',
    'type': 'patent'
}
INDEX_MAPPING[ID_FDA_PRODUCTS] = {
    'index': 'fda_products_v3',
    'type': 'product'
}
INDEX_MAPPING[ID_DERWENT_PATENTS] = {
    'index': 'derwent',
    'type': 'patents'
}
INDEX_MAPPING[ID_DWPI] = {
    'index': 'dwpi',
    'type': 'patent'
}

DERWENT_ID_PREFIX = 'DP'
USPTO_ID_PREFIX = ''


TASK_STATUS_NOT_STARTED = 'TASK_STATUS_NOT_STARTED'
TASK_STATUS_IN_PROGRESS = 'TASK_STATUS_IN_PROGRESS'
TASK_STATUS_COMPLETED   = 'TASK_STATUS_COMPLETED'
