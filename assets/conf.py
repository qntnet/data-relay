import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR

ASSETS_DIR = os.path.join(WORK_DIR, 'assets')
ASSETS_LIST_FILE_NAME = os.path.join(ASSETS_DIR, 'list.json')
ASSETS_LIST_URL = urljoin(MASTER_ADDR, 'master/assets')

ASSETS_DATA_DIR = os.path.join(ASSETS_DIR, 'data')
ASSETS_DATA_VERIFY_URL = urljoin(MASTER_ADDR, 'master/verify-data')

