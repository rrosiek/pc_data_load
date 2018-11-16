from data_load.base.data_mapper import DataMapper
import datetime
import re

class PCDataMapper(DataMapper):

    @staticmethod
    def allow_doc_creation(data_source_name):
        return True

    @staticmethod
    def create_only(data_source_name):
        return False

    @staticmethod
    def get_es_id(_id):
        return _id

    @staticmethod
    def get_doc_id(_id):
        return _id

    @staticmethod
    def pad_zeros(number, count):
        number_str = str(number)
        while len(number_str) < count:
            number_str = '0' + number_str

        return number_str

    @staticmethod
    def add_value_if_not_null(doc, data, key):
        if key in data:
            value = data[key]
            if value != 'NULL':
                doc[key] = value

        return doc

    @staticmethod
    def create_doc(_id, data_source_name, data):
        doc = {}

        data = data[0]

        doc = PCDataMapper.add_value_if_not_null(doc, data, 'InstitutionName')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'InstDUNSNum')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardID')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardIDFromGM')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardProposalID')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardeeID')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardeeEmail')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardeeLastName')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardeeFirstName')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardTitle')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardStartDate')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'AwardEndDate')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'GeneralSummary')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'TechnicalSummary')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'ORCID')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'ProgramName')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'ProgramCycle')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'ProgramAbbreviation')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'ProgramDeadline')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'ProgramOpenDate')
        doc = PCDataMapper.add_value_if_not_null(doc, data, 'GMName')

        if 'AwardAmount' in data:
            award_amount = data['AwardAmount']
            if award_amount != 'NULL':
                award_amount = award_amount.replace('$', '')
            doc['AwardAmount'] = award_amount
        if 'PMIDS' in data:
            pmids = data['PMIDS']
            if pmids != 'NULL':
                pmids = pmids.split(',')
            else:
                pmids = []
            doc['PMIDS'] = pmids
        if 'DOIDS' in data:
            doids = data['DOIDS']
            if doids != 'NULL':
                doids = doids.split(',')
            else:
                doids = []
            doc['DOIDS'] = doids
        
        return doc

    @staticmethod
    def update_doc(existing_doc, _id, data_source_name, data):
        new_doc = PCDataMapper.create_doc(_id, data_source_name, data)
        update_doc = new_doc

        return update_doc
