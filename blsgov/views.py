import datetime
import json
import os
import xarray as xr
import pandas as pd
import numpy as np
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import zipfile

import gzip

from blsgov.conf import *
from blsgov.sync import mk_zip_container_name

DATE_FORMAT = '%Y-%m-%d'


def get_dbs(request,  last_time=None):
    with gzip.open(BLSGOV_DB_LIST_FILE_NAME, 'r') as f:
        dbs = f.read()
    return HttpResponse(dbs, content_type='application/json')


def get_db_meta(request, last_time=None):
    id = request.GET['id']
    with gzip.open(os.path.join(BLSGOV_DIR, id, BLSGOV_META_FILE_NAME), 'rt') as f:
        meta = f.read()
    return HttpResponse(meta, content_type='application/json')


def get_series_meta(request, last_time=None):
    id = request.GET['id']
    last_series = request.GET.get("last_series", "")

    series_files = sorted(os.listdir(os.path.join(BLSGOV_DIR, id, BLSGOV_SERIES_DIR)))

    fn = next((f for f in series_files if f.split(PERIOD_SEPARATOR)[1].split(".")[0] > last_series), None)

    if fn is None:
        return HttpResponse('[]', content_type='application/json')
    else:
        with gzip.open(os.path.join(BLSGOV_DIR, id, BLSGOV_SERIES_DIR, fn), 'rt') as f:
            raw = f.read()
        res = json.loads(raw)
        res = [r for r in res if r['id'] > last_series]
        return HttpResponse(json.dumps(res, indent=1), content_type='application/json')


def get_series_data(request, last_time=None):
    args = request.GET

    id = args['id']

    min_date = args.get('min_date', '2007-01-01')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(2007, 1, 1)

    max_date = args.get('max_date')
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date() if max_date is not None else datetime.date.today()

    if last_time is not None:
        last_time = datetime.datetime.strptime(last_time.split('T')[0], DATE_FORMAT).date()
        if last_time < max_date:
            max_date = last_time

    res = get_data_series(id, max_date, min_date, BLSGOV_SERIES_DATA_FOLDER)

    return HttpResponse(json.dumps(res, indent=1), content_type='application/json')


def get_series_aspect(request, last_time=None):
    args = request.GET

    id = args['id']

    min_date = args.get('min_date', '2007-01-01')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(2007, 1, 1)

    max_date = args.get('max_date')
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date() if max_date is not None else datetime.date.today()

    if last_time is not None:
        last_time = datetime.datetime.strptime(last_time.split('T')[0], DATE_FORMAT).date()
        if last_time < max_date:
            max_date = last_time

    res = get_data_series(id, max_date, min_date, BLSGOV_SERIES_ASPECT_FOLDER)

    return HttpResponse(json.dumps(res, indent=1), content_type='application/json')


def get_data_series(id, max_date, min_date, folder):
    container_fn = mk_zip_container_name(id)
    container_fn = os.path.join(BLSGOV_DIR, id[:2], folder, container_fn)
    res = []
    min_date = min_date.isoformat()
    max_date = max_date.isoformat()
    try:
        with zipfile.ZipFile(container_fn, 'r') as z:
            res = z.read(id + SERIES_DATA_SUFFIX)
        res = res.decode()
        res = json.loads(res)
        res = [r for r in res if min_date <= r['pub_date'] <= max_date]
    except KeyError:
        pass
    except FileNotFoundError:
        pass
    return res


