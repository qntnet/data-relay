import base64
import gzip
import json
import logging
import os
import xarray as xr
import zipfile
import hashlib
import shutil
import portalocker

from blsgov.conf import *
from datarelay.avantage import load_series_daily_adjusted
from datarelay.http import request_with_retry
from datarelay.settings import INDEXES, RELAY_KEY, BLSGOV_DBS



logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync_dbs():
    go = True
    while go:
        go = False

        logger.info("Download blsgov db list...")
        os.makedirs(BLSGOV_DIR, exist_ok=True)

        old_listing = []

        try:
            with gzip.open(BLSGOV_DB_LIST_FILE_NAME, 'rt') as f:
                raw = f.read()
                old_listing = json.loads(raw)
        except:
            logger.exception("can't read " + BLSGOV_DB_LIST_FILE_NAME)

        raw = request_with_retry(BLSGOV_MASTER_URL + 'db/')
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
            go = True

        lockfile = BLSGOV_DB_LIST_FILE_NAME + '.lock'
        with portalocker.Lock(lockfile, flags=portalocker.LOCK_EX):
            with gzip.open(BLSGOV_DB_LIST_FILE_NAME, 'wt') as f:
               f.write(json.dumps(new_listing, indent=1))


def sync_db(id):
    logger.info("sync db: " + id)
    db_folder = os.path.join(BLSGOV_DIR, id.lower())
    db_folder_new = db_folder + "_new"
    lockfile = db_folder + '.lock'

    try:
        shutil.rmtree(db_folder_new)
    except FileNotFoundError:
        pass
    os.makedirs(db_folder_new, exist_ok=True)

    file_list = request_with_retry(BLSGOV_MASTER_URL + "files/" + id.lower())
    file_list = json.loads(file_list)
    file_list = [f['name'] for f in file_list if f['type'] == 'file']

    logger.info("files to download: " + str(len(file_list)))

    for fn in file_list:
        data = request_with_retry(BLSGOV_MASTER_URL + "files/" + id.lower() + "/" + fn)
        path = os.path.join(db_folder_new, fn)
        with open(path, 'wb') as f:
            f.write(data)

    with portalocker.Lock(lockfile, flags=portalocker.LOCK_EX):
        try:
            shutil.rmtree(db_folder)
        except FileNotFoundError:
            pass
        shutil.move(db_folder_new, db_folder)


if __name__ == '__main__':
    sync_dbs()
