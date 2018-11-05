import time
import psutil
import os
from multiprocessing import Process

class BatchDocProcessor(object):
    def __init__(self, ids, process_batch_method, batch_size=1000, process_count=4, process_spawn_delay=0.25):
        self.ids = ids
        self.batch_size = batch_size
        self.process_count = process_count

        self.processes = []
        self.process_batch_method = process_batch_method
        self.process_spawn_delay = process_spawn_delay

    def run(self):
        batch = []

        count = 0
        total_doc_count = len(self.ids)

        for _id in self.ids:
            batch.append(_id)
            count += 1

            if count % self.batch_size == 0:
                progress = ((count/float(total_doc_count)) * 100)

                print '---------------------------------------------------------------------------------------------'
                print 'Progress', count, '/', total_doc_count, progress, '%'
                print '---------------------------------------------------------------------------------------------'
            
            if len(batch) >= self.batch_size:
                self.process_batch(batch)
                batch = []

        if len(batch) > 0:
            self.process_batch(batch)

    def process_batch(self, batch):
        process = Process(target=self.process_batch_method, args=(batch,))
        process.start()

        self.processes.append(process)
        if len(self.processes) >= self.process_count:
            old_process = self.processes.pop(0)
            old_process.join()

        time.sleep(self.process_spawn_delay)
        # self.process_spawn_delay = self.process_spawn_delay * 0.80