import irdb_load_config

import data_load.base.utils.file_utils as file_utils
from data_load.base.constants import *
from data_load.base.utils.process_index import ProcessIndex

from config import *


def process_doc(_id, doc):
    funded = False

    if 'awd_funding' in doc:
        awd_funding = doc['awd_funding']

        if 'total_awarded_amt' in awd_funding:
            total_awarded_amt = float(awd_funding['total_awarded_amt'])
            if total_awarded_amt > 0:
                funded = True
                # print _id, 'funded true'

    updated_doc = {}
    updated_doc['funded'] = funded

    return updated_doc

def run():
    print 'Adding funded flag...'
    process_index = ProcessIndex(SERVER, INDEX, TYPE, process_doc)

    process_index.batch_size = 5000
    process_index.process_count = 16
    process_index.process_spawn_delay = 0.15
    process_index.bulk_data_size = 300000
    
    process_index.run()


import sys
import time

def run_test():
    print __file__, 'running....'
    time.sleep(3)
    print __file__, 'Done'

# run()