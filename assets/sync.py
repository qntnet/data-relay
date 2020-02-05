import base64
import datetime
import gzip
import os
import logging
import json
import urllib.parse
import urllib.request
import time
import sys
import pandas as pd
from datarelay.http import request_with_retry
from datarelay.settings import AVANTAGE_KEY, SYMBOLS
from assets.conf import ASSETS_LIST_URL, ASSETS_LIST_FILE_NAME, ASSETS_DIR, AVANTAGE_DELAY, AVANTAGE_RATE_LIMIT_DELAY, \
    ASSETS_DATA_DIR, ASSETS_DATA_VERIFY_URL
import xarray as xr
import numpy as np
logger = logging.getLogger(__name__)


def sync_list():
    logger.info("Download assets list...")
    os.makedirs(ASSETS_DIR, exist_ok=True)
    listing = []
    min_id = 0
    while True:
        url = ASSETS_LIST_URL + "?min_id=" + str(min_id + 1)
        page = request_with_retry(url)
        page = json.loads(page)
        if len(page) == 0:
            break
        for a in page:
            min_id = max(min_id, a['internal_id'])
            if SYMBOLS is None or a['symbol'] in SYMBOLS:
                listing.append(a)
    with open(ASSETS_LIST_FILE_NAME, 'w') as f:
        f.write(json.dumps(listing, indent=2))
    logger.info('Done.')


def sync_data():
    logger.info("Download data...")
    os.makedirs(ASSETS_DATA_DIR, exist_ok=True)
    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        assets = f.read()
    assets = json.loads(assets)
    for a in assets:
        avantage_data = load_series_daily_adjusted(a['avantage_symbol'])
        if avantage_data is None:
            continue
        # TODO VERIFY WITH OUR API

        d = avantage_data.to_netcdf(compute=True)
        d = gzip.compress(d)
        d = base64.b64encode(d)
        url = ASSETS_DATA_VERIFY_URL + "/" + str(a['internal_id']) + "/"
        approved_range = request_with_retry(url, d)
        if approved_range is None:
            continue
        approved_range = json.loads(approved_range)
        logger.info("approved range " + str(approved_range))
        avantage_data = avantage_data.loc[:, approved_range[0]:approved_range[1]]

        split_cumprod = avantage_data.sel(field="split").cumprod()
        is_liquid = split_cumprod.copy(True)
        is_liquid[:] = 0

        for lr in a['liquid_ranges']:
            try:
                is_liquid.loc[lr[0]:lr[1]] = 1
            except:
                pass

        ext_data = xr.concat([split_cumprod, is_liquid], pd.Index(["split_cumprod", "is_liquid"], name="field"))
        data = xr.concat([avantage_data, ext_data], "field")

        file_name = os.path.join(ASSETS_DATA_DIR, a['id'] + '.nc')
        data.to_netcdf(path=file_name, compute=True)
    logger.info("Done.")


def load_series_daily_adjusted(symbol):
    params = dict()
    params['function'] = 'TIME_SERIES_DAILY_ADJUSTED'
    params['symbol'] = symbol
    params['outputsize'] = 'full'
    res = json_call(**params)
    try:
        res = res['Time Series (Daily)']

        dates = [datetime.datetime.strptime(k, '%Y-%m-%d') for k in res]
        dates.sort()

        result = xr.DataArray(
            np.full([7, len(dates)], np.NaN),
            dims = ['field', 'time'],
            coords = {
                "field": ["open", "low", "high", "close", "vol", "divs", "split"],
                "time": dates,
            }
        )
        result.loc["open"] = [float(res[str(k.date())][OPEN_FIELD]) for k in dates]
        result.loc["close"] = [float(res[str(k.date())][CLOSE_FIELD]) for k in dates]
        result.loc["high"] = [float(res[str(k.date())][HIGH_FIELD]) for k in dates]
        result.loc["low"] = [float(res[str(k.date())][LOW_FIELD]) for k in dates]
        result.loc["vol"] = [float(res[str(k.date())][VOLUME_FIELD]) for k in dates]
        result.loc["divs"] = [float(res[str(k.date())][DIVS_FIELD]) for k in dates]
        result.loc["split"] = [float(res[str(k.date())][SPLIT_FIELD]) for k in dates]

        return result
    except:
        logger.exception("unexpected")
        return None


SPLIT_FIELD = '8. split coefficient'
DIVS_FIELD = '7. dividend amount'
VOLUME_FIELD = '6. volume'
LOW_FIELD = '3. low'
HIGH_FIELD = '2. high'
OPEN_FIELD = '1. open'
CLOSE_FIELD = '4. close'


def json_call(**kwargs):
    params = dict(kwargs)
    raw = rate_limited_raw_call(**params)
    res = parse_json(raw)
    return res


last_call = 0

def rate_limited_raw_call(**kwargs):
    global last_call
    params = dict(kwargs)
    params['apikey'] = AVANTAGE_KEY
    params = urllib.parse.urlencode(params)
    url = 'https://www.alphavantage.co/query?' + params
    while True:
        if time.time() - last_call < AVANTAGE_DELAY:
            time.sleep(AVANTAGE_DELAY - (time.time() - last_call))
        last_call = time.time()
        try:
            # if settings.DEBUG:
            #     print(url)
            logger.info("get " + url)
            response = urllib.request.urlopen(url, timeout=10)
            raw = response.read()
            raw = raw.decode()
        except KeyboardInterrupt:
            raise
        except:
            logger.exception("unexpected")
            continue
        if "https://www.alphavantage.co/premium/" in raw:
            logger.warning("rate limit")
            time.sleep(AVANTAGE_RATE_LIMIT_DELAY)
        else:
            return raw


def parse_json(raw):
    try:
        return json.loads(raw)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        return None


if __name__ == '__main__':
    sync_list()
    sync_data()