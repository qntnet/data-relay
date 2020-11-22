import logging
import os

from cryptofutures.conf import CRYPTOFUTURES_FILE_NAME, CRYPTOFUTURES_URL
from datarelay.http import request_with_retry
from datarelay.settings import WORK_DIR


logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync():
    logger.info("Download crypto...")
    os.makedirs(WORK_DIR, exist_ok=True)
    data = request_with_retry(CRYPTOFUTURES_URL)
    with open(CRYPTOFUTURES_FILE_NAME, 'wb') as f:
        f.write(data)
    logger.info('Done.')


if __name__ == '__main__':
    sync()