import os
import logging
import json
from datarelay.http import load_with_retry
from datarelay.settings import WORK_DIR
from listing.conf import LISTING_URL, LISTING_FILE_NAME


logger = logging.getLogger(__name__)


def sync():
    logger.info("Download listing...")
    os.makedirs(WORK_DIR, exist_ok=True)
    listing = []
    min_id = 0
    while True:
        url = LISTING_URL + "?min_id=" + str(min_id + 1)
        page = load_with_retry(url)
        page = json.loads(page)
        if len(page) == 0:
            break
        for a in page:
            min_id = max(min_id, a['internal_id'])
            listing.append(a)
    with open(LISTING_FILE_NAME, 'w') as f:
        f.write(json.dumps(listing, indent=2))
    logger.info('Done.')