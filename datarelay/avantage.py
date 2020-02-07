import datetime
import json
import sys
import time
import urllib.parse
import urllib.request
import logging
import numpy as np
import xarray as xr
from datarelay.settings import AVANTAGE_KEY, AVANTAGE_DELAY, AVANTAGE_TIMEOUT, AVANTAGE_RATE_LIMIT_DELAY

logger = logging.getLogger(__name__)


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
            response = urllib.request.urlopen(url, timeout=AVANTAGE_TIMEOUT)
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


def load_series_daily_adjusted(symbol):
    params = dict()
    params['function'] = 'TIME_SERIES_DAILY_ADJUSTED'
    params['symbol'] = symbol
    params['outputsize'] = 'full'
    res = json_call(**params)

    if 'Time Series (Daily)' not in res:
        return None

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
