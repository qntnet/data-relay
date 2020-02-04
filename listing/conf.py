import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR

LISTING_FILE_NAME = os.path.join(WORK_DIR, 'assets.json')
LISTING_URL = urljoin(MASTER_ADDR, 'master/assets')