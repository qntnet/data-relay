import base64
import gzip
import os
import logging
import json
from random import shuffle

import pandas as pd

from datarelay.avantage import load_series_daily_adjusted
from datarelay.http import request_with_retry
from datarelay.settings import SYMBOLS, RELAY_KEY
from assets.conf import ASSETS_LIST_URL, ASSETS_LIST_FILE_NAME, ASSETS_DIR, ASSETS_DATA_DIR, ASSETS_DATA_VERIFY_URL, \
    ASSETS_DATA_FULL_URL
import xarray as xr


logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync_list():
    logger.info("Download asset list...")
    os.makedirs(ASSETS_DIR, exist_ok=True)
    listing = []
    symbols_json = json.dumps(SYMBOLS).encode()
    min_id = 0
    while True:
        url = ASSETS_LIST_URL + "?min_id=" + str(min_id + 1)
        page = request_with_retry(url, symbols_json)
        page = json.loads(page)
        if len(page) == 0:
            break
        for a in page:
            min_id = max(min_id, a['internal_id'])
            listing.append(a)
    with open(ASSETS_LIST_FILE_NAME, 'w') as f:
        f.write(json.dumps(listing, indent=2))
    logger.info('Done.')


def sync_data():
    logger.info("Download assets data...")
    os.makedirs(ASSETS_DATA_DIR, exist_ok=True)
    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        assets = f.read()
    assets = json.loads(assets)
    shuffle(assets)
    progress = 0
    for a in assets:
        if RELAY_KEY is None:
            main_data = load_series_daily_adjusted(a['avantage_symbol']) # FORWARD ORDER
            if main_data is None:
                continue
            d = main_data.to_netcdf(compute=True)
            d = gzip.compress(d)
            d = base64.b64encode(d)
            url = ASSETS_DATA_VERIFY_URL + "/" + str(a['internal_id']) + "/"
            approved_range = request_with_retry(url, d)
            if approved_range is None:
                logger.info("approved range None")
                continue
            approved_range = json.loads(approved_range)
            logger.info("approved range " + str(approved_range))

            main_data = main_data.loc[:, approved_range[0]:approved_range[1]]

            split_cumprod = main_data.sel(field="split").cumprod()
            is_liquid = split_cumprod.copy(True)
            is_liquid[:] = 0

            for lr in a['liquid_ranges']:
                try:
                    is_liquid.loc[lr[0]:lr[1]] = 1
                except:
                    pass

            ext_data = xr.concat([split_cumprod, is_liquid], pd.Index(["split_cumprod", "is_liquid"], name="field"))
            data = xr.concat([main_data, ext_data], "field")
        else:
            url = ASSETS_DATA_FULL_URL + "/" + str(a['internal_id']) + "/"
            main_data = request_with_retry(url)
            if main_data is None:
                continue
            data = xr.open_dataarray(main_data)

        file_name = os.path.join(ASSETS_DATA_DIR, a['id'] + '.nc')
        data.to_netcdf(path=file_name, compute=True)
        progress += 1
        logger.info("progress: " + str(progress) + "/" + str(len(assets)))
    logger.info("Done.")


if __name__ == '__main__':
    sync_list()
    sync_data()