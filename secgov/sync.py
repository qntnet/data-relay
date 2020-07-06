import datetime

from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from datarelay.http import request_with_retry
import os
import json
import logging
from secgov.conf import BASE_URL, SECGOV_FORMS_DIR_NAME, SEC_GOV_LAST_ID_FILE_NAME, SECGOV_FACTS_DIR_NAME
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

            r['id'] = r['url'].split('/data/', 1)[1].split('/',2)[1]

            forms_dir = os.path.join(SECGOV_FORMS_DIR_NAME, r['cik'])
            os.makedirs(forms_dir, exist_ok=True)

            report_fn = r['date'] + '$' + r['type'].replace('/', '-') + '$' + r['id'] + ".json.gz"
            report_fn = os.path.join(forms_dir, report_fn)
            with gzip.open(report_fn,'wt') as f:
                f.write(json.dumps(r, indent=1))

            facts_dir = os.path.join(SECGOV_FACTS_DIR_NAME, r['cik'])
            os.makedirs(facts_dir, exist_ok=True)

            facts = r['facts']
            facts = [{
                'cik':r['cik'],
                'report_id':r['id'],
                'report_date':r['date'],
                'report_type':r['type'],
                'report_url':r['url'],

                'fact_name': f['name'],
                'segment': f['segment'],
                'value': f['value'],

                'period': f['period']['value'] if f['period'] is not None else None,
                'period_type': f['period']['type'] if f['period'] is not None else None,
                'period_length': get_period_length(f),

                'unit': f['unit']['value'] if f['unit'] is not None else None,
                'unit_type': f['unit']['type'] if f['unit'] is not None else None,
            } for f in facts]

            facts = list(sorted(facts, key=lambda f: f['fact_name']))
            facts = [(g[0], list(g[1])) for g in groupby(facts, lambda f: f['fact_name'])]

            for (n, ff) in facts:
                fn = os.path.join(facts_dir, n + ".json.list.gz")
                with gzip.open(fn, 'at') as f:
                    for fact in ff:
                        f.write(json.dumps(fact) + "\n")

        with open(SEC_GOV_LAST_ID_FILE_NAME, 'w') as f:
            f.write(str(last_id))
    logger.info("Done.")


def get_period_length(fact):
    if fact['period'] is not None and type(fact['period']['value']) == list:
        start = datetime.datetime.strptime(fact['period']['value'][0], DATE_FORMAT).date()
        end = datetime.datetime.strptime(fact['period']['value'][1], DATE_FORMAT).date()
        return (end - start).days
    else:
        return None


DATE_FORMAT = '%Y-%m-%d'


if __name__ == "__main__":
    sync()