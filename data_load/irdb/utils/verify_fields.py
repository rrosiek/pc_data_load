import json
import os
from config import *

import data_load.base.utils.file_utils as file_utils

fields_mapping = {
    'data_format_header_APPLS_AT.json': GRANTS_FIELDS,
    'data_format_header_APPLS_MV.json': GRANTS_FIELDS,
    'data_format_header_AWD_FUNDINGS_MV.json': AWD_FIELDS,
    'data_format_header_AWD_FUNDINGS_PUB.json': AWD_FIELDS,
    'data_format_header_PVP_GRANT_PI_MV.json': ADD_DATA_FIELDS
}

def run():
    directory = '/data/data_loading/scripts/data-load-n/data_load/irdb/new_sample_data'
    file_format = '.json'

    missing_fields = {}

    for name in os.listdir(directory):
        file_path = os.path.join(directory, name)
        if os.path.isfile(file_path) and name.endswith(file_format):
            data = file_utils.load_file(directory, name)

            if name in fields_mapping:
                fields = fields_mapping[name]

                missing_fields_for_file = []
                for data_item in data:
                    for field in fields:
                        if field not in data_item:
                            missing_fields_for_file.append(field)
                    break

                missing_fields[name] = missing_fields_for_file

    for name in missing_fields:
        print name
        print missing_fields[name]

        print '-----------------------------'


v3_doc_json = '{"subproject_type_code":"C","multi_pi_indicator_code":"N","source_code_dc":"CURRENT","mechanism_code":"RP","esi_appl_elig_flag":"2","admin_phs_org_code":"HD","activity_code":"P01","serial_num":"11149","ic_subproject_id":"9554","appl_type_code":"5","appl_status_code":"05","fy":"2007","grant_num":"P01HD011149-28","subproject_id":"0024","irg_code":"CHHD","appl_class_code":"G","major_activity_code":"P","competing_grant_code":"N","project_title":"TRANSCRIPTIONAL REGULATION OF CERVICAL COMPETENCE DURING PREGNANCY","modular_grant_flag":"N","appl_id":"7343216","support_year":"28"}'
v2_doc_json = '{"stem_cells_used_code":"N","external_org_wip_flag":"N","ss_revision_note_code":"N","esnap_exists_code":"N","m_row$$":"AATQX5ABxAAANouAAS","funded":false,"council_meeting_date":"200600","extension_eligible_code":"N","admin_phs_org_code":"HD","ss_biohazard_flag":"N","appl_status_code":"05","irg_code":"CHHD","summary_statement_exists_code":"N","appl_shell_flag":"N","ss_budgetary_overlap_code":"N","project_title":"TRANSCRIPTIONAL REGULATION OF CERVICAL COMPETENCE DURING PREGNANCY","activity_code":"P01","grant_num":"P01HD011149-28","subproject_type_code":"C","competing_grant_code":"N","wip_terms_status_code":"A","major_activity_code":"P","ss_animal_subject_comment_code":"N","creator_id":"CMSII8","mechanism_code":"RP","last_upd_date":"16-JUL-15","last_upd_id":"IMPACII8","city_state_name":"DALLAS            TEXAS","appl_type_code":"5","ss_budget_comments_flag":"N","ss_admin_note_flag":"N","initial_grant":false,"init_encumbrance_date":"07-APR-05","appl_class_code":"G","fy":"2007","nga_line_item_code":"N","multi_pi_indicator_code":"N","appl_id":"7343216","fsr_accepted_code":"N","modular_grant_flag":"N","abstract_exists_code":"Y","support_year":"28","grant_image_exists_code":"N","appl_status_date":"22-JAN-07","human_subject_code":"N","ss_foreign_flag":"N","ic_approved_code":"Y","icd_disp_code":"9","external_org_id":"578404","subproject_id":"0024","serial_num":"11149","ic_subproject_id":"9554","created_date":"22-JAN-07"}'

def run2():
    v3_doc = json.loads(v3_doc_json)
    v2_doc = json.loads(v2_doc_json)




    extra_fields = []
    for field in v2_doc:
        field_upper = field.upper()
        if (field_upper not in GRANTS_FIELDS) and (field_upper not in AWD_FIELDS) and (field_upper not in ADD_DATA_FIELDS):
            extra_fields.append(field_upper)
        
    print extra_fields

    print '------------------'
    print 'Fields in v3: ', len(v3_doc)
    print 'Fields in v2: ', len(v2_doc)

    print 'Extra fields: ', len(extra_fields)

run2()

