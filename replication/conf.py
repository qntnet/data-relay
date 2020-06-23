import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR, RELAY_KEY, NODE_NAME

STOCKS_LAST_DATE_FILE_NAME = os.path.join(WORK_DIR, 'stocks.dt.txt')
CRYPTO_LAST_DATE_FILE_NAME = os.path.join(WORK_DIR, 'crypto.dt.txt')
FUTURES_LAST_DATE_FILE_NAME = os.path.join(WORK_DIR, 'futures.dt.txt')

STOCKS_LAST_DATE_URL = urljoin(MASTER_ADDR, 'master/last-update')
CRYPTO_LAST_DATE_URL = urljoin(MASTER_ADDR, 'master/crypto-last-update')
FUTURES_LAST_DATE_URL = urljoin(MASTER_ADDR, 'master/futures-last-update')

POST_STATUS_URL = urljoin(MASTER_ADDR, 'master/replication/' + str(RELAY_KEY) + "/" + str(NODE_NAME))

