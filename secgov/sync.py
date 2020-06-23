from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from datarelay.http import request_with_retry
import os
import json
import logging
from secgov.conf import BASE_URL, SECGOV_FORMS_DIR_NAME, SEC_GOV_LAST_ID_FILE_NAME
from datarelay.settings import SECGOV_INCREMENTAL_UPDATE
import gzip
from itertools import groupby

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync():
    logger.info("Sync sec.gov forms...")

    os.makedirs(SECGOV_FORMS_DIR_NAME, exist_ok=True)

    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        tickers = f.read()
    tickers = json.loads(tickers)
    tickers = [t for t in tickers if os.path.exists(os.path.join(ASSETS_DATA_DIR, t['id'] + '.nc'))]
    ciks = [t['cik'] for t in tickers]
    ciks = json.dumps(ciks).encode()

    last_id = 0
    try:
        with open(SEC_GOV_LAST_ID_FILE_NAME, 'r') as f:
            last_id = f.readline()
            last_id = last_id.strip()
            last_id = int(last_id)
    except:
        logger.exception("can't read " + SEC_GOV_LAST_ID_FILE_NAME)

    if not SECGOV_INCREMENTAL_UPDATE:
        last_id = 0


    while True:
        url = BASE_URL + "?min_id=" + str(last_id + 1)
        raw = request_with_retry(url, ciks)
        if raw is None:
            break
        lst = json.loads(raw)
        if len(lst) == 0:
            break
        for r in lst:
            last_id = max(last_id, r['id'])

            d = r['date'].split("-")
            path = os.path.join(SECGOV_FORMS_DIR_NAME, r['type'].replace('/', '-'), *d, r['cik'], r['url'].split('/data/', 1)[1].split('/',2)[1])
            os.makedirs(path, exist_ok=True)
            fn = os.path.join(path, 'meta.json.gz')
            facts = r['facts']
            del r['facts']

            with gzip.open(fn, 'wt') as f:
                r = json.dumps(r)
                #r = r.encode()
                #r = gzip.compress(r)
                f.write(r)

            path = os.path.join(path, "facts")
            os.makedirs(path, exist_ok=True)
            for g in groupby(facts, lambda f:f['name']):
                fn = os.path.join(path, g[0] + '.json.gz')
                ff = list(g[1])
                with gzip.open(fn, 'wt') as f:
                    ff = json.dumps(ff)
                    #ff = ff.encode()
                    #ff = gzip.compress(ff)
                    f.write(ff)

        with open(SEC_GOV_LAST_ID_FILE_NAME, 'w') as f:
            f.write(str(last_id))
    logger.info("Done.")


if __name__ == "__main__":
    sync()