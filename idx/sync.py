import base64
import gzip
import json
import logging
import os

from assets.sync import load_series_daily_adjusted
from datarelay.http import request_with_retry
from datarelay.settings import INDEXES
from idx.conf import IDX_DIR, IDX_LIST_URL, IDX_LIST_FILE_NAME, IDX_DATA_DIR, IDX_DATA_VERIFY_URL

logger = logging.getLogger(__name__)

def sync_list():
    logger.info("Download idx list...")
    os.makedirs(IDX_DIR, exist_ok=True)
    url = IDX_LIST_URL
    listing = request_with_retry(url)
    listing = json.loads(listing)
    listing = [l for l in listing if INDEXES is None or l['id'] in INDEXES]
    with open(IDX_LIST_FILE_NAME, 'w') as f:
        f.write(json.dumps(listing, indent=2))
    logger.info('Done.')


def sync_series():
    logger.info("Download idx data...")
    os.makedirs(IDX_DATA_DIR, exist_ok=True)
    with open(IDX_LIST_FILE_NAME, 'r') as f:
        listing = f.read()
    listing = json.loads(listing)
    for a in listing:
        avantage_data = load_series_daily_adjusted(a['id'])
        if avantage_data is None:
            continue
        avantage_data = avantage_data.sel(field='close')
        d = avantage_data.to_netcdf(compute=True)
        d = gzip.compress(d)
        d = base64.b64encode(d)
        url = IDX_DATA_VERIFY_URL + "/" + str(a['id']) + "/"
        approved_range = request_with_retry(url, d)
        if approved_range is None:
            logger.info("approved range None")
            continue
        approved_range = json.loads(approved_range)
        logger.info("approved range " + str(approved_range))
        data = avantage_data.loc[approved_range[0]:approved_range[1]]
        file_name = os.path.join(IDX_DATA_DIR, a['id'] + '.nc')
        data.to_netcdf(path=file_name, compute=True)
    logger.info("Done.")


if __name__ == '__main__':
    sync_list()
    sync_series()