
# Directories
ROOT_DIRECTORY = '/data/data_loading/pubmed2018'

# Load specific configuration
MAPPING_FILE_PATH = 'mapping.json'

SERVER = 'http://34.201.111.196:9200'

ID_FIELD = ''
ID_PREFIX = ''

INDEX = 'pubmed2018_v2'
TYPE = 'article'

# Get wos titles
WOS_TITLES_QUERY = """select * from wos_titles"""
WOS_TITLES_FILE_NAME = "wos_titles.csv"
WOS_TITLES = "wos_titles"

# Get wos titles
WOS_ADDRESSES_QUERY = """select * from wos_addresses"""
WOS_ADDRESSES_FILE_NAME = "wos_addresses.csv"
WOS_ADDRESSES = "wos_addresses"

# Get wos titles
WOS_ABSTRACTS_QUERY = """select * from wos_abstracts"""
WOS_ABSTRACTS_FILE_NAME = "wos_abstracts.csv"
WOS_ABSTRACTS = "wos_abstracts"

# Get wos titles
WOS_AUTHORS_QUERY = """select * from wos_authors"""
WOS_AUTHORS_FILE_NAME = "wos_authors.csv"
WOS_AUTHORS = "wos_authors"

# Get wos titles
WOS_GRANTS_QUERY = """select * from wos_grants"""
WOS_GRANTS_FILE_NAME = "wos_grants.csv"
WOS_GRANTS = "wos_grants"

# Get wos titles
WOS_KEYWORDS_QUERY = """select * from wos_keywords"""
WOS_KEYWORDS_FILE_NAME = "wos_keywords.csv"
WOS_KEYWORDS = "wos_keywords"

# Get wos titles
WOS_PUBLICATIONS_QUERY = """select * from wos_publications"""
WOS_PUBLICATIONS_FILE_NAME = "wos_publications.csv"
WOS_PUBLICATIONS = "wos_publications"

# Get wos titles
WOS_REFERENCES_QUERY = """select * from wos_references"""
WOS_REFERENCES_FILE_NAME = "wos_references.csv"
WOS_REFERENCES = 'wos_references'

# Get wos titles
WOS_PMID_MAPPING_QUERY = """select * from wos_pmid_mapping"""
WOS_PMID_MAPPING_FILE_NAME = "wos_pmid_mapping.csv"
WOS_PMID_MAPPING = 'wos_pmid_mapping'

# Get wos patent mapping
WOS_PATENT_MAPPING_QUERY = """select * from wos_patent_mapping"""
WOS_PATENT_MAPPING_FILE_NAME = "wos_patent_mapping.csv"
WOS_PATENT_MAPPING = 'wos_patent_mapping'

TABLES_LIST = {}

TABLES_LIST[WOS_TITLES] = {
    "query": WOS_TITLES_QUERY,
    "file_name": WOS_TITLES_FILE_NAME
}

TABLES_LIST[WOS_ADDRESSES] = {
    "query": WOS_ADDRESSES_QUERY,
    "file_name": WOS_ADDRESSES_FILE_NAME
}

TABLES_LIST[WOS_ABSTRACTS] = {
    "query": WOS_ABSTRACTS_QUERY,
    "file_name": WOS_ABSTRACTS_FILE_NAME
}

TABLES_LIST[WOS_AUTHORS] = {
    "query": WOS_AUTHORS_QUERY,
    "file_name": WOS_AUTHORS_FILE_NAME
}

TABLES_LIST[WOS_GRANTS] = {
    "query": WOS_GRANTS_QUERY,
    "file_name": WOS_GRANTS_FILE_NAME
}

TABLES_LIST[WOS_KEYWORDS] = {
    "query": WOS_KEYWORDS_QUERY,
    "file_name": WOS_KEYWORDS_FILE_NAME
}

TABLES_LIST[WOS_PUBLICATIONS] = {
    "query": WOS_PUBLICATIONS_QUERY,
    "file_name": WOS_PUBLICATIONS_FILE_NAME
}

TABLES_LIST[WOS_REFERENCES] = {
    "query": WOS_REFERENCES_QUERY,
    "file_name": WOS_REFERENCES_FILE_NAME
}

TABLES_LIST[WOS_PMID_MAPPING] = {
    "query": WOS_PMID_MAPPING_QUERY,
    "file_name": WOS_PMID_MAPPING_FILE_NAME
}

TABLES_LIST[WOS_PATENT_MAPPING] = {
    "query": WOS_PATENT_MAPPING_QUERY,
    "file_name": WOS_PATENT_MAPPING_FILE_NAME
}


