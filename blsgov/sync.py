import base64
import gzip
import json
import logging
import os
import xarray as xr

from blsgov.conf import *
from datarelay.avantage import load_series_daily_adjusted
from datarelay.http import request_with_retry
from datarelay.settings import INDEXES, RELAY_KEY
from idx.conf import IDX_DIR, IDX_LIST_URL, IDX_LIST_FILE_NAME, IDX_DATA_DIR, IDX_DATA_VERIFY_URL, IDX_DATA_FULL_URL

import glob

PERIOD_SEPARATOR = ".to."

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync_dbs():
    logger.info("Download blsgov db list...")
    os.makedirs(BLSGOV_DIR, exist_ok=True)

    old_listing = []

    try:
        with gzip.open(BLSGOV_DB_LIST_FILE_NAME, 'rt') as f:
            raw = f.read()
            old_listing = json.loads(raw)
    except:
        logger.exception("can't read " + BLSGOV_DB_LIST_FILE_NAME)

    raw = request_with_retry(BLSGOV_DB_LIST_URL)
    new_listing = json.loads(raw)

    # TODO
    # if len(new_listing) == len([l for l in new_listing if l in old_listing]):
    #     logger.info("nothing is new")
    #     return

    for l in new_listing:
        # TODO
        # if l in old_listing:
        #     continue
        sync_db(l['id'])

    with gzip.open(BLSGOV_DB_LIST_FILE_NAME, 'wt') as f:
       f.write(json.dumps(new_listing, indent=1))


def sync_db(id):
    logger.info("sync db: " + id)
    db_folder = os.path.join(BLSGOV_DIR, id)
    os.makedirs(db_folder, exist_ok=True)
    sync_db_meta(db_folder, id)
    series_dir = os.path.join(db_folder, BLSGOV_SERIES_DIR)
    sync_series(series_dir,  id)
    sync_db_series_data(db_folder, series_dir, BLSGOV_DB_SERIES_DATA_URL, BLSGOV_SERIES_DATA_FOLDER)
    sync_db_series_data(db_folder, series_dir, BLSGOV_DB_SERIES_ASPECT_URL, BLSGOV_SERIES_ASPECT_FOLDER)


def sync_series(series_dir, id):
    os.makedirs(series_dir, exist_ok=True)
    for old_fn in os.listdir(series_dir):
        os.remove(os.path.join(series_dir, old_fn))
    last_series = ''
    while True:
        url = BLSGOV_DB_SERIES_URL + "?id=" + id + "&last_series=" + last_series
        series = request_with_retry(url)
        series = json.loads(series)
        if len(series) == 0:
            break
        first_series = series[0]['id']
        last_series = series[-1]['id']
        with gzip.open(os.path.join(series_dir, first_series + PERIOD_SEPARATOR + last_series + ".json.gz"), 'wt') as f:
            f.write(json.dumps(series, indent=1))


def sync_db_meta(db_folder, id):
    url = BLSGOV_DB_META_URL + "?id=" + id
    meta = request_with_retry(url)
    with gzip.open(os.path.join(db_folder, BLSGOV_META_FILE_NAME), 'wb') as f:
        f.write(meta)


def sync_db_series_data(db_folder, series_dir, base_url, dir):
    for sfn in glob.glob(os.path.join(series_dir, '*')):
        with gzip.open(sfn, 'rt') as f:
            series = f.read()
            series = json.loads(series)
        for s in series:
            series_dt = s['end_year'] + s['end_period']
            data_dir = mk_data_dir(s['id'])
            data_dir = os.path.join(db_folder, dir, data_dir)
            os.makedirs(data_dir, exist_ok=True)
            changed = True
            for fn in os.listdir(data_dir):
                if not os.path.isfile(os.path.join(data_dir,fn)):
                    continue
                pp = fn.split('.')
                if pp[0] != s['id']:
                    continue
                if pp[1] == series_dt:
                    changed = False
                else:
                    os.remove(os.path.join(data_dir,fn))
            if not changed:
                continue
            data_fn = os.path.join(data_dir, s['id'] + '.' + series_dt + ".json.gz")
            data = request_with_retry(base_url + "?id=" + s['id'])
            #print(data)
            with gzip.open(data_fn, 'wb') as zf:
                # print(data_fn, data)
                zf.write(data)
                zf.flush()

def mk_data_dir(id):
    id = [i[0] + i[1] for i in  zip(id[::2], id[1::2])]
    return os.path.join(*id[:-1])


if __name__ == '__main__':
    sync_dbs()
