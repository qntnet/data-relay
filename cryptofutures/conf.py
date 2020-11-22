import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR

CRYPTOFUTURES_FILE_NAME = os.path.join(WORK_DIR, 'cryptofutures.nc')
CRYPTOFUTURES_URL = urljoin(MASTER_ADDR, 'master/cryptofutures')
