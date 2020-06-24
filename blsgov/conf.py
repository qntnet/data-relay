import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR, RELAY_KEY

BLSGOV_DIR = os.path.join(WORK_DIR, 'blsgov')
BLSGOV_DB_LIST_FILE_NAME = os.path.join(BLSGOV_DIR, 'dbs.json.gz')
BLSGOV_META_FILE_NAME = 'meta.json.gz'
BLSGOV_SERIES_DIR = 'series'
BLSGOV_SERIES_DATA_FOLDER = 'data'
BLSGOV_SERIES_ASPECT_FOLDER = 'aspect'

BLSGOV_DB_LIST_URL = urljoin(MASTER_ADDR, 'master/bls.gov/db/list')
BLSGOV_DB_META_URL = urljoin(MASTER_ADDR, 'master/bls.gov/db/meta')
BLSGOV_DB_SERIES_URL = urljoin(MASTER_ADDR, 'master/bls.gov/series/list')
BLSGOV_DB_SERIES_DATA_URL = urljoin(MASTER_ADDR, 'master/bls.gov/series/data')
BLSGOV_DB_SERIES_ASPECT_URL = urljoin(MASTER_ADDR, 'master/bls.gov/series/aspect')


SERIES_DATA_SUFFIX = '.data.json'
SERIES_LAST_DT_SUFFIX = '.dt.txt'
PERIOD_SEPARATOR = ".to."
