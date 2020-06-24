import base64
import gzip
import json
import logging
import os
import xarray as xr
import zipfile
import hashlib
import shutil

from blsgov.conf import *
from datarelay.avantage import load_series_daily_adjusted
from datarelay.http import request_with_retry
from datarelay.settings import INDEXES, RELAY_KEY, BLSGOV_DBS

import itertools


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
    if BLSGOV_DBS is not None:
        new_listing = [l for l in new_listing if l['id'] in BLSGOV_DBS]

    if len(new_listing) == len([l for l in new_listing if l in old_listing]):
        logger.info("nothing is new")
        return

    for l in new_listing:
        if l in old_listing:
             continue
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

    sync_db_series_data(os.path.join(db_folder, BLSGOV_SERIES_DATA_FOLDER), series_dir, BLSGOV_DB_SERIES_DATA_URL)
    sync_db_series_data(os.path.join(db_folder, BLSGOV_SERIES_ASPECT_FOLDER), series_dir, BLSGOV_DB_SERIES_ASPECT_URL)


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


def sync_db_series_data(data_dir, series_dir, base_url):
    os.makedirs(data_dir, exist_ok=True)

    need_to_remove = dict()
    need_to_add = dict()

    old_series_dts = dict()

    # TODO corrupted files

    for fn in os.listdir(data_dir):
        with zipfile.ZipFile(os.path.join(data_dir, fn), 'r') as zf:
            for item in zf.infolist():
                if item.filename.endswith(SERIES_LAST_DT_SUFFIX):
                    old_series_dts[item.filename[:-len(SERIES_LAST_DT_SUFFIX)]] = zf.read(item.filename).decode()

    for sfn in os.listdir(series_dir):
        with gzip.open(os.path.join(series_dir, sfn), 'rt') as f:
            series = f.read()
            series = json.loads(series)
        for s in series:
            id = s['id']
            new_dt = s['end_year'] + s['end_period']
            old_dt = old_series_dts.get(id, '')

            if old_dt != '':
                if old_dt == new_dt:
                    continue
                else:
                    zip_container_fn = mk_zip_container_name(id)
                    zip_container_fn = os.path.join(data_dir, zip_container_fn)
                    if zip_container_fn not in need_to_remove:
                        need_to_remove[zip_container_fn] = set()
                    need_to_remove[zip_container_fn].add(id)

            need_to_add[id] = new_dt

    # rm old series
    for (k, v) in need_to_remove.items():
        delete_from_zip_container(k, set(itertools.chain.from_iterable((i + SERIES_DATA_SUFFIX, i + SERIES_LAST_DT_SUFFIX) for i in v)))

    # load new series
    for (id,series_dt) in need_to_add.items():
        zip_container_fn = mk_zip_container_name(id)
        zip_container_fn = os.path.join(data_dir, zip_container_fn)

        data_fn = id + SERIES_DATA_SUFFIX
        meta_fn = id + SERIES_LAST_DT_SUFFIX

        data = request_with_retry(base_url + "?id=" + id)
        data = data.decode()
        data = json.loads(data)
        data = json.dumps(data, indent=1)

        with zipfile.ZipFile(zip_container_fn, 'a', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.writestr(data_fn, data)
            zf.writestr(meta_fn, series_dt)


def mk_zip_container_name(id):
    m = hashlib.shake_128()
    m.update(id.encode())
    return m.hexdigest(1) + '.zip'


def delete_from_zip_container(zip_container, files_to_delete):
    logger.info("delete " + zip_container + " " + str(files_to_delete))
    newfn = zip_container + '.new'
    with zipfile.ZipFile(zip_container, 'r') as zin:
        with zipfile.ZipFile(newfn, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zout:
            for item in zin.infolist():
                buffer = zin.read(item.filename)
                if item.filename not in files_to_delete:
                    zout.writestr(item, buffer)
    shutil.move(newfn, zip_container)


if __name__ == '__main__':
    sync_dbs()
