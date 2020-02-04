import datetime
import json

from django.http import HttpResponse

from listing.conf import LISTING_FILE_NAME

DATE_FORMAT = '%Y-%m-%d'


def get_assets(request):
    min_date = request.GET.get('min_date', '2007-01-01')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date()

    max_date = request.GET.get('max_date', datetime.date.today().isoformat())
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date()

    with open(LISTING_FILE_NAME, 'r') as f:
        tickers = f.read()
    tickers = json.loads(tickers)
    tickers = [t for t in tickers if is_liquid_in_dates(t, min_date, max_date)]
    for t in tickers:
        del t['internal_id']
        del t['liquid_ranges']

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