from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from datarelay.http import request_with_retry
import os
import json
import logging
from secgov.conf import BASE_URL, SECGOV_FORMS_DIR_NAME #, SEC_GOV_LAST_ID_FILE_NAME


logger = logging.getLogger(__name__)

# TODO LIMIT BY ASSETS
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
    # try:
    #     with open(SEC_GOV_LAST_ID_FILE_NAME, 'r') as f:
    #         last_id = f.readline()
    #         last_id = last_id.strip()
    #         last_id = int(last_id)
    # except:
    #     logger.exception("can't read " + SEC_GOV_LAST_ID_FILE_NAME)
    while True:
        url = BASE_URL + "?min_id=" + str(last_id + 1)
        raw = request_with_retry(url, ciks)
        if raw is None:
            break
        lst = json.loads(raw)
        if len(lst) == 0:
            break
        for r in lst:
            d = r['date'].split("-")
            path = os.path.join(SECGOV_FORMS_DIR_NAME, r['type'].replace('/', '-'), *d, r['cik'])
            os.makedirs(path, exist_ok=True)
            fn = os.path.join(path, str(r['id']) + '.json')
            with open(fn, 'w') as f:
                f.write(json.dumps(r, indent=2))
            last_id = max(last_id, r['id'])
        # with open(SEC_GOV_LAST_ID_FILE_NAME, 'w') as f:
        #     f.write(str(last_id))
    logger.info("Done.")


if __name__ == "__main__":
    sync()