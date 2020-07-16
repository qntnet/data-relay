import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR

BLSGOV_DIR = os.path.join(WORK_DIR, 'blsgov2')
BLSGOV_DB_LIST_FILE_NAME = os.path.join(BLSGOV_DIR, 'list.json.gz')
BLSGOV_META_FILE_NAME = 'meta.json.gz'

BLSGOV_MASTER_URL =  urljoin(MASTER_ADDR, 'bls.gov/api/')
