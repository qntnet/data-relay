import datetime

from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from datarelay.http import request_with_retry
import os
import json
import logging
from secgov.conf import BASE_URL, SEC_GOV_IDX_FILE_NAME, SECGOV_BASE_DIR, SEC_GOV_CONTENT_FILE_NAME, \
    SEC_GOV_FACTS_FILE_NAME
import gzip
import zipfile

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')


def sync():
    logger.info("Sync sec.gov forms...")

    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        tickers = f.read()
    tickers = json.loads(tickers)
    # tickers = [t for t in tickers if os.path.exists(os.path.join(ASSETS_DATA_DIR, t['id'] + '.nc'))]
    ciks = [t['cik'] for t in tickers]
    ciks = [c for c in ciks if c is not None]
    # print(ciks)
    os.makedirs(SECGOV_BASE_DIR, exist_ok=True)

    idx = request_with_retry(BASE_URL + SEC_GOV_IDX_FILE_NAME)
    idx = gzip.decompress(idx)
    idx = json.loads(idx)
    idx = [i for i in idx if i['cik'] in ciks]
    # print(idx)

    old_idx = []

    try:
        with gzip.open(os.path.join(SECGOV_BASE_DIR, SEC_GOV_IDX_FILE_NAME), 'rt') as f:
            txt = f.read()
            old_idx = json.loads(txt)
    except:
        logger.exception("cant load " + SEC_GOV_IDX_FILE_NAME)

    if idx == old_idx:
        logger.info("nothing changed")
        return

    for i in idx:
        logger.info("sync: " + json.dumps(i))
        oi = next((j for j in old_idx if j['cik'] == i['cik']), None)
        if oi == i:
            print("not changed")
            continue

        os.makedirs(os.path.join(SECGOV_BASE_DIR, i['cik']), exist_ok=True)

        raw = request_with_retry(BASE_URL + i['cik'] + "/" + SEC_GOV_IDX_FILE_NAME)
        with open(os.path.join(SECGOV_BASE_DIR, i['cik'], SEC_GOV_IDX_FILE_NAME), 'wb') as f:
            f.write(raw)

        raw = request_with_retry(BASE_URL + i['cik'] + "/" + SEC_GOV_CONTENT_FILE_NAME)
        with open(os.path.join(SECGOV_BASE_DIR, i['cik'], SEC_GOV_CONTENT_FILE_NAME), 'wb') as f:
            f.write(raw)

        facts = dict()
        with zipfile.ZipFile(os.path.join(SECGOV_BASE_DIR, i['cik'], SEC_GOV_CONTENT_FILE_NAME), 'r') as z:
            for n in z.namelist():
                r = z.read(n).decode()
                r = json.loads(r)
                for f in r['facts']:
                    _f = {
                        'cik':r['cik'],
                        'report_id':r['id'],
                        'report_date':r['date'],
                        'report_type':r['type'],
                        'report_url':r['url'],

                        'fact_name': f['name'],
                        'segment': f.get('segment'), # TODO
                        'value': f['value'],

                        'period': f['period']['value'] if f.get('period') is not None else None, # TODO
                        'period_type': f['period']['type'] if f.get('period') is not None else None,
                        'period_length': get_period_length(f),

                        'unit': f['unit']['value'] if f.get('unit') is not None else None, # TODO
                        'unit_type': f['unit']['type'] if f.get('unit') is not None else None,
                    }
                    if f['name'] not in facts.keys():
                        facts[f['name']] = []
                    facts[f['name']].append(_f)

        with zipfile.ZipFile(os.path.join(SECGOV_BASE_DIR, i['cik'], SEC_GOV_FACTS_FILE_NAME), 'w',
                             compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            for k in facts.keys():
                p = facts[k]
                p.sort(key = lambda i: (i['report_date'], i['report_id']))
                z.writestr(k + '.json', json.dumps(p, indent=1))

    with gzip.open(os.path.join(SECGOV_BASE_DIR, SEC_GOV_IDX_FILE_NAME), 'wt') as f:
        f.write(json.dumps(idx, indent=1))

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