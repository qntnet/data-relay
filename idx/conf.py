import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR

IDX_DIR = os.path.join(WORK_DIR, 'idx')
IDX_LIST_FILE_NAME = os.path.join(IDX_DIR, 'list.json')
IDX_LIST_URL = urljoin(MASTER_ADDR, 'master/idx-list')

IDX_DATA_DIR = os.path.join(IDX_DIR, 'data')
IDX_DATA_VERIFY_URL = urljoin(MASTER_ADDR, 'master/idx-verify')
