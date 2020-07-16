import datetime
import json
import os
import xarray as xr
import pandas as pd
import numpy as np
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import zipfile
import portalocker
import gzip

from blsgov.conf import *

DATE_FORMAT = '%Y-%m-%d'


def get_dbs(request,  last_time=None):
    with portalocker.Lock(BLSGOV_DB_LIST_FILE_NAME + '.lock', flags=portalocker.LOCK_SH | portalocker.LOCK_NB):
        with gzip.open(BLSGOV_DB_LIST_FILE_NAME, 'r') as f:
            dbs = f.read()
    return HttpResponse(dbs, content_type='application/json')


def get_db_meta(request, last_time=None):
    id = request.GET['id']
    db_path = os.path.join(BLSGOV_DIR, id.lower())

    with portalocker.Lock(db_path + '.lock', flags=portalocker.LOCK_SH | portalocker.LOCK_NB):
        with gzip.open(os.path.join(db_path, BLSGOV_META_FILE_NAME), 'rt') as f:
            meta = f.read()
        return HttpResponse(meta, content_type='application/json')


def get_series_meta(request, last_time=None):
    id = request.GET['id']
    last_series_id = request.GET.get("last_series", "")
    db_path = os.path.join(BLSGOV_DIR, id.lower())

    with portalocker.Lock(db_path + '.lock', flags=portalocker.LOCK_SH | portalocker.LOCK_NB):
        files = os.listdir(db_path)
        files = [{
            "from": f.split('.')[1],
            "to": f.split('.')[2],
            "name": f
        } for f in files if f.startswith('series.')]
        files.sort(key=lambda f: f['from'])
        if last_series_id is None:
            series_file = files[0]
        else:
            series_file = next((f for f in files if f['to'] > last_series_id), None)
        if series_file is None:
            return HttpResponse('[]', content_type='application/json')

        series_path = os.path.join(db_path, series_file['name'])
        with gzip.open(series_path, 'rt') as f:
            series = f.read()
        series = json.loads(series)

        if last_series_id is not None:
            series = [s for s in series if s['id'] > last_series_id]

        return HttpResponse(json.dumps(series, indent=1), content_type='application/json')


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

    res = get_data_series(id, max_date, min_date, 'data')

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

    res = get_data_series(id, max_date, min_date, 'aspect')

    return HttpResponse(json.dumps(res, indent=1), content_type='application/json')


def get_data_series(id, max_date, min_date, prefix):
    db_id = id[:2].lower()

    prefix = prefix + '.'
    db_path = os.path.join(BLSGOV_DIR, db_id.lower())

    with portalocker.Lock(db_path + '.lock', flags=portalocker.LOCK_SH | portalocker.LOCK_NB):
        files = os.listdir(db_path)
        files = [{
            "from": f.split('.')[1],
            "to": f.split('.')[2],
            "name": f
        } for f in files if f.startswith(prefix)]
        data_file = next((f for f in files if f['from'] <= id <= f['to']), None)
        if data_file is None:
            return []
        path = os.path.join(db_path, data_file['name'])
        with zipfile.ZipFile(path, 'r') as z:
            try:
                content = z.read(id + '.json')
            except KeyError:
                return []
        content = content.decode()
        content = json.loads(content)

        for p in content:
            p['pub_date'] = get_pub_date(p)
        content = [p for p in content if min_date <= p['pub_date'] <= max_date]
        for p in content:
            p['pub_date'] = p['pub_date'].isoformat()

        return content


def get_pub_date(p):
    PUB_OFFSET = 14
    if p['period'][0] == 'A':
        return datetime.date(p['year'] + 1, 1, 1) - datetime.timedelta(days=1) + datetime.timedelta(days=PUB_OFFSET)
    month = None
    if p['period'][0] == 'M':
        month = int(p['period'][1:])
    elif p['period'][0] == 'Q':
        month = int(p['period'][1:]) * 3
    elif p['period'][0] == 'S':
        month = int(p['period'][1:]) * 6
    else:
        raise Exception("wrong period")
    if month >= 12:
        return datetime.date(p['year'] + 1, 1, 1) - datetime.timedelta(days=1) + datetime.timedelta(days=PUB_OFFSET)
    else:
        return datetime.date(p['year'], month + 1, 1) - datetime.timedelta(days=1) + datetime.timedelta(days=PUB_OFFSET)