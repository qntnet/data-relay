import os
from urllib.parse import urljoin

from datarelay.settings import MASTER_ADDR, WORK_DIR

CRYPTO_FILE_NAME = os.path.join(WORK_DIR, 'crypto.nc')
CRYPTO_URL = urljoin(MASTER_ADDR, 'master/crypto')

