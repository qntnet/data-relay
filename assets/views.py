import datetime
import json
import os
import xarray as xr
import pandas as pd
import numpy as np
from django.http import HttpResponse
from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR

DATE_FORMAT = '%Y-%m-%d'


def get_assets(request):
    min_date = request.GET.get('min_date', '2007-01-01')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date()

    max_date = request.GET.get('max_date', datetime.date.today().isoformat())
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date()

    with open(ASSETS_LIST_FILE_NAME, 'r') as f:
        tickers = f.read()
    tickers = json.loads(tickers)
    tickers = [t for t in tickers if is_liquid_in_dates(t, min_date, max_date)]
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


def get_data(request):
    str_body = request.body.decode()
    dict = json.loads(str_body)

    asset_ids = dict['assets']

    min_date = dict['min_date']
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date()

    max_date = dict['max_date']
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date()

    if min_date > max_date:
        return HttpResponse('wrong dates: min_date > max_date', status_code=400)

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
        part = part.loc[:, max_date:min_date]
        if len(part.coords['time']) == 0:
            continue
        if output is None:
            output = part
        else:
            part.name = a.external_id
            output.append(part)

    if len(output) == 0:
        return HttpResponse('', content_type='application/x-netcdf')

    output = xr.concat(output, pd.Index([a.name for a in output], name='asset'))
    output = output.dropna('time', 'all')
    output = output.transpose('field', 'time', 'asset')
    output = output.loc[:, np.sort(output.coords['time'].values)[::-1], np.sort(output.coords['asset'].values)]
    output = output.to_netcdf(compute=True)

    response = HttpResponse(output, content_type='application/x-netcdf')
    return response
