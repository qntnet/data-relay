import os
import logging
import json
from datarelay.http import request_with_retry

from futures.conf import FUTURES_DIR, FUTURES_LIST_URL, FUTURES_LIST_FILE_NAME, FUTURES_DATA_URL, FUTURES_DATA_FILE_NAME

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync_list():
    logger.info("Download futures list...")
    os.makedirs(FUTURES_DIR, exist_ok=True)
    url = FUTURES_LIST_URL
    lst = request_with_retry(url)
    lst = json.loads(lst)
    with open(FUTURES_LIST_FILE_NAME, 'w') as f:
        f.write(json.dumps(lst, indent=2))
    logger.info('Done.')


def sync_data():
    logger.info("Download futures data...")
    os.makedirs(FUTURES_DIR, exist_ok=True)
    url = FUTURES_DATA_URL
    data = request_with_retry(url)
    with open(FUTURES_DATA_FILE_NAME, 'wb') as f:
        f.write(data)
    logger.info('Done.')


if __name__ == '__main__':
    sync_list()
    sync_data()