import datetime
import json
import os
import xarray as xr
import pandas as pd
import numpy as np
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from idx.conf import IDX_LIST_FILE_NAME, IDX_DATA_DIR, MAJOR_IDX_LIST_FILE_NAME, MAJOR_IDX_DATA_FILE_NAME
from replication.conf import STOCKS_LAST_DATE_FILE_NAME

DATE_FORMAT = '%Y-%m-%d'


def get_major_list(request,  last_time=None):
    with open(MAJOR_IDX_LIST_FILE_NAME, 'r') as f:
        raw = f.read()
    tickers = json.loads(raw)
    for t in tickers:
        try:
            del t['last_time']
        except Exception as ignored:
            pass
    return HttpResponse(json.dumps(tickers), content_type='application/json')


def get_major_data(request,  last_time=None):
    min_date = request.GET.get('min_date', '2007-01-01')
    max_date = request.GET.get('max_date', datetime.date.today().isoformat())

    if last_time is not None:
        last_time = last_time.split('T')[0]
        if last_time < max_date:
            max_date = last_time

    try:
        with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
            max_allowed_date = f.read()
            if max_date > max_allowed_date:
                max_date = max_allowed_date
    except:
        pass

    output = xr.open_dataarray(MAJOR_IDX_DATA_FILE_NAME, cache=True, decode_times=True)
    output = output.transpose('field', 'time', 'asset')
    output = output.loc[:, max_date:min_date]
    output = output.loc[:, np.sort(output.time.values)[::-1], np.sort(output.asset.values)]
    output = output.to_netcdf(compute=True)
    return HttpResponse(output, content_type='application/x-netcdf')


def get_idx_list(request,  last_time=None):
    min_date = request.GET.get('min_date', '2007-01-01')
    max_date = request.GET.get('max_date', datetime.date.today().isoformat())

    if last_time is not None:
        last_time = last_time.split('T')[0]
        if last_time < max_date:
            max_date = last_time

    try:
        with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
            max_allowed_date = f.read()
            if max_date > max_allowed_date:
                max_date = max_allowed_date
    except:
        pass

    with open(IDX_LIST_FILE_NAME, 'r') as f:
        tickers = f.read()
    tickers = json.loads(tickers)
    tickers = [t for t in tickers if os.path.exists(os.path.join(IDX_DATA_DIR, t['id'] + '.nc'))]
    tickers = [t for t in tickers if len(xr.open_dataarray(os.path.join(IDX_DATA_DIR, t['id'] + '.nc'), cache=True, decode_times=True).loc[min_date:max_date]) > 0]
    tickers.sort(key = lambda t: t['id'])
    for t in tickers:
        try:
            del t['last_time']
        except Exception as ignored:
            pass
        try:
            del t['etf']
        except Exception as ignored:
            pass
    str_tickers = json.dumps(tickers)
    return HttpResponse(str_tickers, content_type='application/json')


@csrf_exempt
def get_idx_data(request,  last_time=None):
    str_body = request.body.decode()
    dict = json.loads(str_body)

    ids = dict['ids']

    min_date = request.GET.get('min_date', '2007-01-01')
    max_date = request.GET.get('max_date', datetime.date.today().isoformat())

    if min_date > max_date:
        return HttpResponse('wrong dates: min_date > max_date', status_code=400)

    if last_time is not None:
        last_time = last_time.split('T')[0]
        if last_time < max_date:
            max_date = last_time

    try:
        with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
            max_allowed_date = f.read()
            if max_date > max_allowed_date:
                max_date = max_allowed_date
    except:
        pass

    ids.sort()

    with open(IDX_LIST_FILE_NAME, 'r') as f:
        idx_list = f.read()
    idx_list = json.loads(idx_list)
    idx_list = [a for a in idx_list if a['id'] in ids]
    idx_list.sort(key = lambda a: a['id'])

    output = []
    for a in idx_list:
        fn = os.path.join(IDX_DATA_DIR, a['id'] + '.nc')
        if not os.path.exists(fn):
            continue
        part = xr.open_dataarray(fn, cache=True, decode_times=True)
        part = part.compute()
        part = part.loc[min_date:max_date]
        if len(part.time) == 0:
            continue
        part.name = a['id']
        output.append(part)

    if len(output) == 0:
        return HttpResponse('', content_type='application/x-netcdf')

    output = xr.concat(output, pd.Index([a.name for a in output], name='asset'))
    output = output.dropna('time', 'all')
    output = output.transpose('time', 'asset')
    output = output.loc[np.sort(output.coords['time'].values)[::-1], np.sort(output.coords['asset'].values)]
    output = output.to_netcdf(compute=True)

    response = HttpResponse(output, content_type='application/x-netcdf')
    return response
