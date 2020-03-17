import datetime
import os
import json

from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from secgov.conf import SECGOV_FORMS_DIR_NAME

DATE_FORMAT = '%Y-%m-%d'


@csrf_exempt
def get_sec_gov_forms(request, last_time=None):
    str_body = request.body.decode()
    args = json.loads(str_body)

    ciks = args.get('ciks')
    types = args.get('types')
    facts = args.get('facts')
    offset = args.get('offset', 0)
    skip_segment = args.get('skip_segment', False)

    min_date = args.get('min_date', '2007-01-01')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date()

    max_date = args.get('max_date', datetime.date.today().isoformat())
    max_date = datetime.datetime.strptime(max_date, DATE_FORMAT).date()

    if last_time is not None:
        last_time = datetime.datetime.strptime(last_time.split('T')[0], DATE_FORMAT).date()
        if last_time < max_date:
            max_date = last_time

    limit = 20
    if facts is not None:
        limit = 20000 // (len(facts) * 10 + 1)

    page = get_fillings(types, ciks, facts, skip_segment, min_date, max_date, offset, limit)

    #print(len(page))
    resp = json.dumps(page)
    return HttpResponse(resp, content_type='application/json')


def get_fillings(types=None, ciks=None, facts=None, skip_segment=None, min_date=None, max_date=None, offset=0, limit=1):
    if types is not None:
        types = [t.replace('/', '-') for t in types]
    file_names = []
    for root, dirs, files in os.walk(SECGOV_FORMS_DIR_NAME):
        if len(files) == 0:
            continue
        rp = root.split(os.sep)
        if ciks != None and rp[-1] not in ciks:
            continue
        if types != None and rp[-5] not in types:
            continue
        d = datetime.date(int(rp[-4]), int(rp[-3]), int(rp[-2]))
        if min_date is not None and d < min_date:
            continue
        if max_date is not None and d > max_date:
            continue
        for fn in files:
            full = os.path.join(root, fn)
            file_names.append(full)
    file_names.sort()
    file_names = file_names[offset:][:limit]
    result = []
    for fn in file_names:
        with open(fn, 'r') as f:
            raw = f.read()
        obj = json.loads(raw)
        if facts is not None:
            obj['facts'] = [f for f in obj['facts'] if f['name'] in facts]
        if skip_segment is not None and skip_segment:
            obj['facts'] = [f for f in obj['facts'] if f['segment'] is None]
        result.append(obj)
    return result


if __name__ == '__main__':
    f = get_fillings(
        facts=['us-gaap:AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount'],
        max_date=datetime.date(2013, 7, 1),
        skip_segment=True,
        limit=200
    )
    print(f)

