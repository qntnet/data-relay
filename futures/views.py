import datetime
import json
import os
import xarray as xr
import pandas as pd
import numpy as np
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from assets.conf import ASSETS_LIST_FILE_NAME, ASSETS_DATA_DIR
from futures.conf import FUTURES_LIST_FILE_NAME, FUTURES_DATA_FILE_NAME, FUTURES_ENABLED
from replication.conf import STOCKS_LAST_DATE_FILE_NAME, FUTURES_LAST_DATE_FILE_NAME

DATE_FORMAT = '%Y-%m-%d'


def get_list(request, last_time=None):
    if not FUTURES_ENABLED:
        return HttpResponse("[]", content_type='application/json', status=200)
    with open(FUTURES_LIST_FILE_NAME, 'r') as f:
        lst = f.read()
    return HttpResponse(lst, content_type='application/json')


def get_data(request, last_time=None):
    if not FUTURES_ENABLED:
        return HttpResponse("", content_type='text/plain')

    args = request.GET

    min_date = args.get('min_date')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(1990, 1, 1)

    max_date = args.get('max_date')
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date() if max_date is not None else datetime.date.today()

    if min_date > max_date:
        return HttpResponse('wrong dates: min_date > max_date', status_code=400)

    if last_time is not None:
        last_time = datetime.datetime.strptime(last_time.split('T')[0], DATE_FORMAT).date()
        if last_time < max_date:
            max_date = last_time

    try:
        with open(FUTURES_LAST_DATE_FILE_NAME, 'r') as f:
            max_allowed_date = f.read()
            max_allowed_date = datetime.datetime.strptime(max_allowed_date, DATE_FORMAT)
            if max_date > max_allowed_date:
                max_date = max_allowed_date
    except:
        pass

    arr = xr.open_dataarray(FUTURES_DATA_FILE_NAME, cache=True, decode_times=True)
    arr = arr.sel(time=slice(max_date.isoformat(), min_date.isoformat(), None))
    output = arr.to_netcdf(compute=True)

    return HttpResponse(output, content_type='application/x-netcdf')
