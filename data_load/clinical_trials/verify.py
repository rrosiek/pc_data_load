import requests
import json
import ct_load_config

import data_load.base.utils.file_utils as file_utils

data_sources = [
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


def run():
    load_config = ct_load_config.get_load_config()
    generated_files_dir = load_config.generated_files_directory()

    for data_source in data_sources:
        unique_ids = 0
        try:
            data_source_dir = generated_files_dir + '/' + data_source
            stats_file = data_source + '_stats.json'


            stats = file_utils.load_file(data_source_dir, stats_file)
            unique_ids = stats['unique_ids']
        except Exception as e:
            pass

        # print 'verifing', data_source
        url = 'http://localhost:9200/clinical_trials/study/_search?size=0'

        data = {
            "query": {
                "exists": {
                    "field": data_source
                }
            }
        }

        response = requests.post(url, json=data)
        if response.status_code == 200 or response.status_code == 201:
            response_obj = json.loads(response.text)
            total = response_obj['hits']['total']

            percent_completion = 0
            if unique_ids > 0:
                percent_completion = (total / float(unique_ids)) * 100
            print data_source, '- [', total, '/', unique_ids, '] -', percent_completion, '%'
            print '----------------------------------------------------------------------------------'

        else:
            print response.text



run()
