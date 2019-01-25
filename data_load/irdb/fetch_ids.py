from data_load.base.utils.export_doc_ids import export_doc_ids



SERVER = 'http://localhost:9200'
INDEX = 'irdb_v3'
TYPE = 'grant'


def run():
    doc_ids = export_doc_ids(server=SERVER, src_index=INDEX, src_type=TYPE)
    doc_ids.sort()

    print len(doc_ids)
    print doc_ids[-4]
    print doc_ids[-3]
    print doc_ids[-2]
    print doc_ids[-1]

run()
