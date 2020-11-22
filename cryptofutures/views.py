import datetime
import xarray as xr
from django.http import HttpResponse

from cryptofutures.conf import CRYPTOFUTURES_FILE_NAME

DATE_FORMAT = '%Y-%m-%d'


def get_cryptofutures_series(request, last_time=None):
    min_date = request.GET.get('min_date')
    if min_date is None:
        min_date = '2007-01-01'
    min_date = parse_datetime(min_date)

    max_date = request.GET.get('max_date')
    if max_date is None:
        max_date = datetime.datetime.now()
    else:
        max_date = parse_datetime(max_date)

    if last_time is not None:
        last_time = parse_datetime(last_time)
        if last_time < max_date:
            max_date = last_time

    data = xr.open_dataarray(CRYPTOFUTURES_FILE_NAME)
    data = data.sel(time=slice(max_date,min_date))

    ncdf = data.to_netcdf(compute=True)
    return HttpResponse(ncdf, content_type='application/x-netcdf')


def parse_datetime(dtstr):
    try:
        return datetime.datetime.strptime(dtstr, '%Y-%m-%d')
    except:
        return datetime.datetime.strptime(dtstr, '%Y-%m-%dT%H')