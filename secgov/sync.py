from datarelay.http import load_with_retry
import os
import json
import logging
from secgov.conf import SEC_GOV_LAST_ID_FILE_NAME, BASE_URL, SECGOV_FORMS_DIR_NAME


logger = logging.getLogger(__name__)

# TODO LIMIT BY ASSETS
def sync():
    logger.info("Sync sec.gov forms...")
    last_id = 0
    try:
        with open(SEC_GOV_LAST_ID_FILE_NAME, 'r') as f:
            last_id = f.readline()
            last_id = last_id.strip()
            last_id = int(last_id)
    except:
        logger.exception("can't read " + SEC_GOV_LAST_ID_FILE_NAME)
    while True:
        url = BASE_URL + "?min_id=" + str(last_id + 1)
        raw = load_with_retry(url)
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
        with open(SEC_GOV_LAST_ID_FILE_NAME, 'w') as f:
            f.write(str(last_id))
    logger.info("Done.")


if __name__ == "__main__":
    sync()