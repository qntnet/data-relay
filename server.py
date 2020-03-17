from datarelay import wsgi

import bjoern
import os
from multiprocessing import Process

HOST = '0.0.0.0'
PORT = int(os.environ.get('RELAY_PORT', '7070'))
NUM_WORKERS = int(os.environ.get('WORKER_COUNT', '2'))

workers = []

print("Start server " + HOST + ":" + str(PORT))

bjoern.listen(wsgi.application, HOST, PORT)

def run(number):
    try:
        print("start worker #" + str(number))
        bjoern.run()
    except KeyboardInterrupt:
        pass

for i in range(NUM_WORKERS):
    p = Process(target = run, args = (i,))
    p.daemon = True
    p.start()
    workers.append(p)

try:
    for p in workers:
        p.join()
except KeyboardInterrupt:
    print("Interrupt signal.")
    pass

print("Exit.")
