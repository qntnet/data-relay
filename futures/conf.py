import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR, RELAY_KEY

FUTURES_ENABLED = os.environ.get("FUTURES_ENABLED", "true") == "true"
FUTURES_DIR = os.path.join(WORK_DIR, 'futures')
FUTURES_LIST_FILE_NAME = os.path.join(FUTURES_DIR, 'list.json')
FUTURES_LIST_URL = urljoin(MASTER_ADDR, 'master/futures/list')

FUTURES_DATA_FILE_NAME = os.path.join(FUTURES_DIR, 'data.nc')
FUTURES_DATA_URL = urljoin(MASTER_ADDR, 'master/futures/data/' + str(RELAY_KEY))
