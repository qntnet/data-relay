import base64
import gzip
import json
import logging
import os
import xarray as xr

from datarelay.avantage import load_series_daily_adjusted
from datarelay.http import request_with_retry
from datarelay.settings import INDEXES, RELAY_KEY
from idx.conf import IDX_DIR, IDX_LIST_URL, IDX_LIST_FILE_NAME, IDX_DATA_DIR, IDX_DATA_VERIFY_URL, IDX_DATA_FULL_URL, \
    MAJOR_IDX_LIST_URL, MAJOR_IDX_LIST_FILE_NAME, MAJOR_IDX_DATA_URL, MAJOR_IDX_DATA_FILE_NAME

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync_major():
    if RELAY_KEY is None:
        return
    os.makedirs(IDX_DIR, exist_ok=True)
    logger.info("Download major idx list...")
    listing = request_with_retry(MAJOR_IDX_LIST_URL)

    old_listing = ''

    try:
        with open(MAJOR_IDX_LIST_FILE_NAME, 'rb') as f:
            old_listing = f.read()
    except FileNotFoundError:
        pass

    if old_listing == listing:
        logger.info("nothing changed")
        #return

    with open(MAJOR_IDX_LIST_FILE_NAME, 'wb') as f:
        f.write(listing)

    logger.info("Download major idx data...")
    data = request_with_retry(MAJOR_IDX_DATA_URL)

    with open(MAJOR_IDX_DATA_FILE_NAME, 'wb') as f:
        f.write(data)


def sync_indexes():
    logger.info("Download idx list...")
    os.makedirs(IDX_DIR, exist_ok=True)
    url = IDX_LIST_URL
    listing = request_with_retry(url)
    listing = json.loads(listing)
    listing = [l for l in listing if INDEXES is None or l['id'] in INDEXES]

    old_listing = []

    try:
        with open(IDX_LIST_FILE_NAME, 'rb') as f:
            old_listing = f.read()
            old_listing = json.loads(old_listing)
    except FileNotFoundError:
        pass

    if listing == old_listing:
        logger.info('nothing changed')
        #return

    with open(IDX_LIST_FILE_NAME, 'w') as f:
        f.write(json.dumps(listing, indent=2))
    logger.info('Done.')

    logger.info("Download idx data...")
    os.makedirs(IDX_DATA_DIR, exist_ok=True)
    with open(IDX_LIST_FILE_NAME, 'r') as f:
        listing = f.read()
    listing = json.loads(listing)
    for a in listing:
        if RELAY_KEY is None:
            if a.get('etf') is None:
                continue
            avantage_data = load_series_daily_adjusted(a['etf'])
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
        else:
            url = IDX_DATA_FULL_URL + "/" + str(a['id']) + "/"
            data = request_with_retry(url)
            if data is None:
                continue
            data = xr.open_dataarray(data)
        file_name = os.path.join(IDX_DATA_DIR, a['id'] + '.nc')
        data.to_netcdf(path=file_name, compute=True)
    logger.info("Done.")


if __name__ == '__main__':
    sync_major()
    sync_indexes()
    # sync_series()