SERVER = 'http://localhost:9200'

# Directories
ROOT_DIRECTORY = '/data/data_loading/clinical_trials_09Jul2018'

# Load specific configuration
MAPPING_FILE_PATH = 'mapping.json'

ID_FIELD = 'nct_id'
ID_PREFIX = ''

INDEX = 'clinical_trials_v2'
TYPE = 'study'

# Batch Size & Process Count
DATA_LOADER_BATCH_SIZE = 2500
DATA_SOURCE_BATCH_SIZE = 1000000

DOC_FETCH_BATCH_SIZE = 500

PROCESS_COUNT = 8
PROCESS_SPAWN_DELAY = 2  # seconds

BULK_DATA_SIZE = 20000000

CT_TABLES = [
    "ct_clinical_studies",
    "ct_arm_groups",
    "ct_authorities",
    "ct_collaborators",
    "ct_condition_browses",
    "ct_conditions",
    "ct_intervention_arm_group_labels",
    "ct_intervention_browses",
    "ct_intervention_other_names",
    "ct_interventions",
    "ct_keywords",
    "ct_links",
    "ct_location_countries",
    "ct_location_investigators",
    "ct_locations",
    "ct_outcomes",
    "ct_overall_contacts",
    "ct_overall_officials",
    "ct_publications",
    # "ct_pubmed_pmid",
    "ct_references",
    "ct_secondary_ids",
]

DATA_SOURCES = [
    "ct_clinical_studies",
    "ct_arm_groups",
    "ct_authorities",
    "ct_collaborators",
    "ct_condition_browses",
    "ct_conditions",
    "ct_intervention_arm_group_labels",
    "ct_intervention_browses",
    "ct_intervention_other_names",
    "ct_interventions",
    "ct_keywords",
    "ct_links",
    "ct_location_countries",
    "ct_location_investigators",
    "ct_locations",
    "ct_outcomes",
    "ct_overall_contacts",
    "ct_overall_officials",
    "ct_publications",
    # "ct_pubmed_pmid",
    "ct_secondary_ids",
]

