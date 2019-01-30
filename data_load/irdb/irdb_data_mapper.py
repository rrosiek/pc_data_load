from data_load.base.data_mapper import DataMapper
from config import *

import time

class IRDBDataMapper(DataMapper):

    @staticmethod
    def allow_doc_creation(data_source_name):
        if (data_source_name == DATA_SOURCE_APPLS_AT or data_source_name == DATA_SOURCE_APPLS_MV):
            return True
        elif data_source_name == DATA_SOURCE_PVA_GRANT_PI_MV:
            return False
        elif data_source_name == DATA_SOURCE_ABSTRACTS or data_source_name == DATA_SOURCE_ABSTRACTS_ARCHIVED:
            return False
        elif data_source_name == DATA_SOURCE_AWD_FUNDINGS or data_source_name == DATA_SOURCE_AWD_FUNDINGS_PUB or data_source_name == DATA_SOURCE_AWD_FUNDINGS_VW:
            return False
        elif data_source_name == DATA_SOURCE_APPL_DC_BUDGETS_MV:
            return False

    @staticmethod
    def create_only(data_source_name):
        return False

    @staticmethod
    def get_es_id(_id):
        return ID_PREFIX + _id

    @staticmethod
    def get_doc_id(_id):
        return _id.replace(ID_PREFIX, '')

    @staticmethod
    def create_doc(_id, data_source_name, data):
        doc = {}
        funded = False

        if (data_source_name == DATA_SOURCE_APPLS_AT or data_source_name == DATA_SOURCE_APPLS_MV):
            for data_item in data:
                for field in GRANTS_FIELDS:
                    doc = IRDBDataMapper.add_value_for_key(field.lower(), doc, data_item)
                
                break

        elif data_source_name == DATA_SOURCE_PVA_GRANT_PI_MV:
            for data_item in data:
                for field in ADD_DATA_FIELDS:
                    doc = IRDBDataMapper.add_value_for_key(field.lower(), doc, data_item)
            
                break

        elif data_source_name == DATA_SOURCE_ABSTRACTS or data_source_name == DATA_SOURCE_ABSTRACTS_ARCHIVED:
            for data_item in data:
                doc = IRDBDataMapper.add_value_for_key('ABSTRACT_TEXT'.lower(), doc, data_item)

                break

        elif data_source_name == DATA_SOURCE_AWD_FUNDINGS or data_source_name == DATA_SOURCE_AWD_FUNDINGS_PUB or data_source_name == DATA_SOURCE_AWD_FUNDINGS_VW:
            for data_item in data:
                awd_funding = {}
                for field in AWD_FIELDS:
                    awd_funding = IRDBDataMapper.add_value_for_key(field.lower(), awd_funding, data_item)

                # Match found
                if len(awd_funding) > 0:
                    doc['awd_funding'] = awd_funding

                    if 'total_awarded_amt' in awd_funding:
                        total_awarded_amt = float(awd_funding['total_awarded_amt'])
                        if total_awarded_amt > 0:
                            funded = True
                    break
        
        if len(doc) > 0:
            doc['funded'] = funded

        return doc

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        doc = {}
        funded = False

        if (data_source_name == DATA_SOURCE_APPLS_AT or data_source_name == DATA_SOURCE_APPLS_MV):
            for data_item in data:
                for field in GRANTS_FIELDS:
                    doc = IRDBDataMapper.add_value_for_key(field.lower(), doc, data_item)
                
                break

        elif data_source_name == DATA_SOURCE_PVA_GRANT_PI_MV:
            for data_item in data:
                for field in ADD_DATA_FIELDS:
                    doc = IRDBDataMapper.add_value_for_key(field.lower(), doc, data_item)
            
                break

        elif data_source_name == DATA_SOURCE_ABSTRACTS or data_source_name == DATA_SOURCE_ABSTRACTS_ARCHIVED:
            for data_item in data:
                doc = IRDBDataMapper.add_value_for_key('ABSTRACT_TEXT'.lower(), doc, data_item)

                break

        elif data_source_name == DATA_SOURCE_AWD_FUNDINGS or data_source_name == DATA_SOURCE_AWD_FUNDINGS_PUB or data_source_name == DATA_SOURCE_AWD_FUNDINGS_VW:
            for data_item in data:
                awd_funding = {}
                for field in AWD_FIELDS:
                    awd_funding = IRDBDataMapper.add_value_for_key(field.lower(), awd_funding, data_item)

                awd_funding_fy = None
                existing_doc_fy = None
                if 'fy' in awd_funding:
                    awd_funding_fy = awd_funding['fy']

                if 'fy' in existing_doc:
                    existing_doc_fy = existing_doc['fy']

                if awd_funding_fy is not None and existing_doc_fy is not None:
                    if awd_funding_fy == existing_doc_fy:
                        # Match found
                        if len(awd_funding) > 0:
                            doc['awd_funding'] = awd_funding

                            if 'total_awarded_amt' in awd_funding:
                                total_awarded_amt = float(awd_funding['total_awarded_amt'])
                                if total_awarded_amt > 0:
                                    funded = True
                            break

        elif data_source_name == DATA_SOURCE_APPL_DC_BUDGETS_MV:
            if 'subproject_id' in existing_doc:
                max_allocated_amt = 0 
                if 'awd_funding' in existing_doc:
                    awd_funding = existing_doc['awd_funding']
                    if 'total_awarded_amt' in awd_funding:
                        total_awarded_amt = awd_funding['total_awarded_amt']
                        if len(total_awarded_amt) > 0:
                            max_allocated_amt = float(total_awarded_amt)

                for data_item in data:
                    if 'ALLOCATED_AMT' in data_item:
                        allocated_amt = data_item['ALLOCATED_AMT']
                        if len(allocated_amt) > 0:
                            allocated_amt = float(allocated_amt)
                        else:
                            allocated_amt = 0
                        if allocated_amt > max_allocated_amt:
                            max_allocated_amt = allocated_amt

                if max_allocated_amt > 0:
                    awd_funding = {}
                    if 'awd_funding' in existing_doc:
                        awd_funding = existing_doc['awd_funding']

                    total_oblgtd_amt = 0
                    if 'total_oblgtd_amt' in awd_funding:
                        total_oblgtd_amt = awd_funding['total_oblgtd_amt']
                        if len(total_oblgtd_amt) > 0:
                            total_oblgtd_amt = float(total_oblgtd_amt)
                        
                    total_awarded_amt = 0
                    if 'total_awarded_amt' in awd_funding:
                        total_awarded_amt = awd_funding['total_awarded_amt']
                        if len(total_awarded_amt) > 0:
                            total_awarded_amt = float(total_awarded_amt)

                    if max_allocated_amt > total_oblgtd_amt: 
                        awd_funding['total_oblgtd_amt'] = str(max_allocated_amt)

                    if max_allocated_amt > total_awarded_amt: 
                        awd_funding['total_awarded_amt'] = str(max_allocated_amt)

                    doc['awd_funding'] = awd_funding
                    funded = True

        if len(doc) > 0:
            doc['funded'] = funded 
        doc = IRDBDataMapper.merge_dict(existing_doc, doc)

        return doc
