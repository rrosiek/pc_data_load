from data_load.base.utils.export_doc_ids import export_doc_ids
from data_load.base.data_source_csv import CSVDataSource


SERVER = 'http://localhost:9200'
INDEX = 'irdb_v3'
TYPE = 'grant'


def run():
    doc_ids = export_doc_ids(server=SERVER, src_index=INDEX, src_type=TYPE)
    
    doc_ids = doc_ids.keys()
    doc_ids.sort()

    print len(doc_ids)
    print doc_ids[-4]
    print doc_ids[-3]
    print doc_ids[-2]
    print doc_ids[-1]

irdb_ids = {}


def process_data():

    appls_at = '/data/data_loading/old-records/irdb_2018_06/source_files/h/APPLS_AT.csv'
    appls_mv = '/data/data_loading/old-records/irdb_2018_06/source_files/h/APPLS_MV.csv'

    print 'processing', appls_at
    csv_data_source = CSVDataSource(appls_at)
    csv_data_source.process_rows(process_row)

    print 'processing', appls_mv
    csv_data_source1 = CSVDataSource(appls_mv)
    csv_data_source1.process_rows(process_row)

    print len(irdb_ids), 'total ids'
    sorted_ids = irdb_ids.keys()
    sorted_ids.sort()

    print sorted_ids[-3]
    print sorted_ids[-2]
    print sorted_ids[-1]

    id_to_search = '9726225'
    if id_to_search in irdb_ids:
        print id_to_search, irdb_ids[id_to_search]
    else:
        print id_to_search, 'not found'

    id_to_search = '9554780'
    if id_to_search in irdb_ids:
        print id_to_search, irdb_ids[id_to_search]
    else:
        print id_to_search, 'not found'


def process_row(row, current_index):
    appl_id = None
    if 'APPL_ID' in row:
        appl_id = str(row['APPL_ID'])

    grant_num = ""
    if 'GRANT_NUM' in row:
        grant_num = row['GRANT_NUM']

    if appl_id is not None:
        if appl_id not in irdb_ids:
            irdb_ids[appl_id] = []

        irdb_ids[appl_id].append(grant_num)

    if current_index % 1000 == 0:
        print 'Processed', current_index, 'ids', appl_id

    return True

process_data()


