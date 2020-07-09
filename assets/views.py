import datetime
import json
import os
import xarray as xr
import pandas as pd
import numpy as np
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from replication.conf import STOCKS_LAST_DATE_FILE_NAME

DATE_FORMAT = '%Y-%m-%d'


def get_assets(request, last_time=None):
    min_date = request.GET.get('min_date')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(2007, 1, 1)

    max_date = request.GET.get('max_date')
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date() if max_date is not None else datetime.date.today()

    if last_time is not None:
        last_time = datetime.datetime.strptime(last_time.split('T')[0], DATE_FORMAT).date()
        if last_time < max_date:
            max_date = last_time

    try:
        with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
            max_allowed_date = f.read()
            max_allowed_date = datetime.datetime.strptime(max_allowed_date, DATE_FORMAT)
            if max_date > max_allowed_date:
                max_date = max_allowed_date
    except:
        pass

    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        tickers = f.read()
    tickers = json.loads(tickers)
    tickers = [t for t in tickers if is_liquid_in_dates(t, min_date, max_date)]
    tickers = [t for t in tickers if os.path.exists(os.path.join(ASSETS_DATA_DIR, t['id'] + '.nc'))]
    for t in tickers:
        del t['internal_id']
        del t['liquid_ranges']
        del t['avantage_symbol']

    tickers.sort(key = lambda t: t['id'])
    str_tickers = json.dumps(tickers)
    return HttpResponse(str_tickers, content_type='application/json')


def is_liquid_in_dates(asset, d1,  d2):
    for r in asset['liquid_ranges']:
        r0 = datetime.datetime.strptime(r[0], DATE_FORMAT).date()
        r1 = datetime.datetime.strptime(r[1], DATE_FORMAT).date()
        if date_ranges_intersect(r0, r1, d1, d2):
            return True
    return False


def date_ranges_intersect(d11, d12, d21, d22):
    return d11 < d21 < d12 or d11 < d22 < d12 or d21 < d11 < d22 or d21 < d12 < d22


MAX_DATA_POINTS_PER_REQUEST = 10*1000*1000

@csrf_exempt
def get_data(request, last_time=None):
    str_body = request.body.decode()
    dict = json.loads(str_body)

    asset_ids = dict['assets']

    min_date = dict.get('min_date')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(2007, 1, 1)

    max_date = dict.get('max_date')
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date() if max_date is not None else datetime.date.today()

    if min_date > max_date:
        return HttpResponse('wrong dates: min_date > max_date', status_code=400)

    if last_time is not None:
        last_time = datetime.datetime.strptime(last_time.split('T')[0], DATE_FORMAT).date()
        if last_time < max_date:
            max_date = last_time

    try:
        with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
            max_allowed_date = f.read()
            max_allowed_date = datetime.datetime.strptime(max_allowed_date, DATE_FORMAT)
            if max_date > max_allowed_date:
                max_date = max_allowed_date
    except:
        pass

    days = (max_date - min_date).days + 1

    asset_count = len(asset_ids)

    if days * asset_count > MAX_DATA_POINTS_PER_REQUEST:
        return HttpResponse('wrong data length: days * asset_count > ' + str(MAX_DATA_POINTS_PER_REQUEST), status_code=400)

    asset_ids.sort()

    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        assets = f.read()
    assets = json.loads(assets)
    assets = [a for a in assets if a['id'] in asset_ids]
    assets.sort(key = lambda a: a['id'])

    output = []
    for a in assets:
        fn = os.path.join(ASSETS_DATA_DIR, a['id'] + '.nc')
        if not os.path.exists(fn):
            continue
        part = xr.open_dataarray(fn, cache=True, decode_times=True)
        part = part.compute()
        part = part.loc[:, min_date.isoformat():max_date.isoformat()]
        if len(part.time) == 0:
            continue
        part.name = a['id']
        output.append(part)

    if len(output) == 0:
        return HttpResponse('', content_type='application/x-netcdf')

    output = xr.concat(output, pd.Index([a.name for a in output], name='asset'))
    output = output.dropna('time', 'all')
    output = output.transpose('field', 'time', 'asset')
    output = output.loc[:, np.sort(output.coords['time'].values)[::-1], np.sort(output.coords['asset'].values)]

    # TODO fix is_liquid column
    output.loc[{'field':'is_liquid'}] = output.loc[{'field':'is_liquid'}].fillna(0).where(output.loc[{'field':'close'}] > 0)

    output = output.to_netcdf(compute=True)

    response = HttpResponse(output, content_type='application/x-netcdf')
    return response
