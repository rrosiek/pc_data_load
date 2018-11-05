import requests
from data_load.base.constants import ID_FDA_PATENTS
from data_load.base.constants import RELATIONSHIP_TYPE_CITATIONS, RELATIONSHIP_TYPE_CITED_BYS, RELATIONSHIP_TYPE_RELATIONS
from multiprocessing import Process
import math

def clear_cited_bys(server, src_index, src_type, source, dest_index_ids):
    print 'Modifying index', src_index, 'with type', src_type, 'clearing cited_bys to', dest_index_ids, 'and source', source

    url = server + '/' + src_index + '/' + src_type + '/' + '_update_by_query?wait_for_completion=false'
    data = {
        "script": {
            'inline': "if (ctx._source.containsKey(\"cited_bys\")) { "
            "     for(int i=0; i<ctx._source.cited_bys.size(); i++) { "
            "         for(int j=0; j<params.index_ids.size(); j++) { "
            "             if(ctx._source.cited_bys[i].index_id == params.index_ids[j] && "
            "                ctx._source.cited_bys[i].source == params.source) { "
            "                 ctx._source.cited_bys[i].ids = params.cited_bys; "
            "                 break "
            "             } "
            "         }"
            "     } "
            "} ",
            "params": {
                "cited_bys": [],
                "index_ids": dest_index_ids,
                "source": source
            }
        }
    }

    response = requests.post(url, json=data)
    print(str(response.status_code))
    print(str(response.text))
    if response.status_code == 200 or response.status_code == 201:
        # print('Updated doc: ' + doc_id)
        return True

    return False

        # "query": {
        #     "exists": {
        #         "field": "citations"
        #     }
        # }

def clear_citations(server, src_index, src_type, source, dest_index_ids):
    print 'Modifying index', src_index, 'with type', src_type, 'clearing citations to', dest_index_ids, 'and source', source

    url = server + '/' + src_index + '/' + src_type + '/' + '_update_by_query?wait_for_completion=false'
    data = {
        "script": {
            'inline': "if (ctx._source.containsKey(\"citations\")) { "
            "     for(int i=0; i<ctx._source.citations.size(); i++) { "
            "         for(int j=0; j<params.index_ids.size(); j++) { "
            "             if(ctx._source.citations[i].index_id == params.index_ids[j] && "
            "                ctx._source.citations[i].source == params.source) { "
            "                 ctx._source.citations[i].ids = params.citations; "
            "                 break "
            "             } "
            "         }"
            "     } "
            "} ",
            "params": {
                "citations": [],
                "index_ids": dest_index_ids,
                "source": source
            }
        }
    }

    response = requests.post(url, json=data)
    print(str(response.status_code))
    print(str(response.text))
    if response.status_code == 200 or response.status_code == 201:
        # print('Updated doc: ' + doc_id)
        return True

    return False

def clear_relations(server, src_index, src_type, source, dest_index_ids):
    print 'Modifying index', src_index, 'with type', src_type, 'clearing relations to', dest_index_ids, 'and source', source 

    url = server + '/' + src_index + '/' + src_type + '/' + '_update_by_query?wait_for_completion=false'
    data = {
        "script": {
            'inline': "if (ctx._source.containsKey(\"relations\")) { "
            "     for(int i=0; i<ctx._source.relations.size(); i++) { "
            "         for(int j=0; j<params.index_ids.size(); j++) { "
            "             if(ctx._source.relations[i].index_id == params.index_ids[j] && "
            "                ctx._source.relations[i].source == params.source) { "
            "                 ctx._source.relations[i].ids = params.relations; "
            "                 break "
            "             } "
            "         }"
            "     } "
            "} ",
            "params": {
                "relations": [],
                "index_ids": dest_index_ids,
                "source": source
            }
        }
    }
  
    response = requests.post(url, json=data)
    print(str(response.status_code))
    print(str(response.text))
    if response.status_code == 200 or response.status_code == 201:
        # print('Updated doc: ' + doc_id)
        return True

    return False


def clear_relations_for_id(server, _id, src_index, src_type, source, dest_index_ids):
    print(dest_index_ids)

    url = server + '/' + src_index + '/' + src_type + '/' + str(_id) + '/' + '_update'
    data = {
        "script": {
            'inline': "if (ctx._source.containsKey(\"relations\")) { "
            "     for(int i=0; i<ctx._source.relations.size(); i++) { "
            "         for(int j=0; j<params.index_ids.size(); j++) { "
            "             if(ctx._source.relations[i].index_id == params.index_ids[j] && "
            "                ctx._source.relations[i].source == params.source) { "
            "                 ctx._source.relations[i].ids = params.relations; "
            "                 break "
            "             } "
            "         }"
            "     } "
            "} ",
            "params": {
                "relations": [],
                "index_ids": dest_index_ids,
                "source": source
            }
        }
    }

    print('Updating doc: ' + _id)
  
    response = requests.post(url, json=data)
    print(str(response.status_code))
    print(str(response.text))
    if response.status_code == 200 or response.status_code == 201:
        # print('Updated doc: ' + doc_id)
        return True

    return False

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

def batch_clear_relations_for_ids(server, _ids, src_index, src_type, source, dest_index_ids, relationship_types, batch_size=1000, process_count=16):
    print 'Clearing', relationship_types, 'to', dest_index_ids, 'for', len(_ids), 'docs in index', src_index, '/', src_type
    session = requests.Session()

    batches = chunks(_ids, batch_size)
    print 'Total batches', math.ceil(len(_ids) / float(batch_size))
    count = 0
    processes = []
    for batch in batches:
        count += 1
        process = Process(target=clear_relations_for_ids, args=(session, server, batch, src_index, src_type, source, dest_index_ids, relationship_types))
        process.start()
        processes.append(process)
  
        if len(processes) >= process_count:
            old_process = processes.pop(0)
            old_process.join()

        print '------------------------------------------------------------------------------'
        print 'Processing batch:', count, (count * batch_size)
        print '------------------------------------------------------------------------------'


def clear_relations_for_ids(session, server, _ids, src_index, src_type, source, dest_index_ids, relationship_types):
    count = 0
    url = server + '/' + src_index + '/' + src_type + '/' + '_update_by_query'
    data = clear_relations_script(_ids, source, dest_index_ids, relationship_types)

    response = session.post(url, json=data)
    if response.status_code == 200 or response.status_code == 201:
        count += len(_ids)
        # if count % 500 == 0:
        #     print(str(count) + 'Updated doc: ' + _id)
        # return True
    else:
        print(str(response.status_code))
        print(str(response.text))
        print('Failed doc: ' + _id)

    print 'Updated', count, 'docs'

def clear_relations_script(_ids, source, dest_index_ids, relationship_types):
    data = {
        "script": {
            'inline': "for(int z=0; z<params.rel_types.size(); z++) { "
            "   if (params.rel_types[z] == \"relations\" && ctx._source.containsKey(\"relations\")) { "
            "       for(int i=0; i<ctx._source.relations.size(); i++) { "
            "           for(int j=0; j<params.index_ids.size(); j++) { "
            "               if(ctx._source.relations[i].index_id == params.index_ids[j] && "
            "                ctx._source.relations[i].source == params.source) { "
            "                   ctx._source.relations[i].ids = params.ids; "
            "               } "
            "           } "
            "       } "
            "   } else if (params.rel_types[z] == \"citations\" && ctx._source.containsKey(\"citations\")) { "
            "       for(int i=0; i<ctx._source.citations.size(); i++) { "
            "           for(int j=0; j<params.index_ids.size(); j++) { "
            "               if(ctx._source.citations[i].index_id == params.index_ids[j] && "
            "                ctx._source.citations[i].source == params.source) { "
            "                   ctx._source.citations[i].ids = params.ids; "
            "               } "
            "           } "
            "       } "
            "   } else if (params.rel_types[z] == \"cited_bys\" && ctx._source.containsKey(\"cited_bys\")) { "
            "       for(int i=0; i<ctx._source.cited_bys.size(); i++) { "
            "           for(int j=0; j<params.index_ids.size(); j++) { "
            "               if(ctx._source.cited_bys[i].index_id == params.index_ids[j] && "
            "                ctx._source.cited_bys[i].source == params.source) { "
            "                   ctx._source.cited_bys[i].ids = params.ids; "
            "               } "
            "           } "
            "       } "
            "   } "
            "} ",
            "params": {
                "rel_types": relationship_types,
                "ids": [],
                "index_ids": dest_index_ids,
                "source": source
            }
        },
        "query": {
            "ids": {
                "values": _ids
            }
        }
    }
    return data

def run():
    server = 'http://localhost:9200'
    src_index = 'uspto_v5'
    src_type = 'grant'
    source = ''
    dest_index_ids = [ID_FDA_PATENTS]

    clear_relations(server, src_index, src_type, source, dest_index_ids)
