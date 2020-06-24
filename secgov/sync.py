from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from datarelay.http import request_with_retry
import os
import json
import logging
from secgov.conf import BASE_URL, SECGOV_FORMS_DIR_NAME, SEC_GOV_LAST_ID_FILE_NAME
from datarelay.settings import SECGOV_INCREMENTAL_UPDATE
import gzip
import zipfile
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

            facts = r['facts']
            del r['facts']

            os.makedirs(os.path.join(SECGOV_FORMS_DIR_NAME, r['cik']), exist_ok=True)

            container_fn = r['date'] + '$' + r['type'].replace('/', '-') + '$' + r['url'].split('/data/', 1)[1].split('/',2)[1] + ".zip"
            container_fn = os.path.join(SECGOV_FORMS_DIR_NAME, r['cik'], container_fn)

            with zipfile.ZipFile(container_fn, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
                r = json.dumps(r, indent=1)
                z.writestr('meta.json', r)
                facts = list(sorted(facts, key=lambda f: f['name']))
                facts = [(g[0], list(g[1])) for g in groupby(facts, lambda f: f['name'])]
                for g in facts:
                    fn = "facts/" +  g[0] + '.json'
                    ff = list(g[1])
                    ff = json.dumps(ff, indent=1)
                    z.writestr(fn, ff)

        with open(SEC_GOV_LAST_ID_FILE_NAME, 'w') as f:
            f.write(str(last_id))
    logger.info("Done.")


if __name__ == "__main__":
    sync()