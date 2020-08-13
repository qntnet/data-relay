import datetime
import os
import json
import time
import gzip
import sys
import zipfile

from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from replication.conf import STOCKS_LAST_DATE_FILE_NAME
from secgov.conf import SEC_GOV_IDX_FILE_NAME, SEC_GOV_CONTENT_FILE_NAME, SEC_GOV_FACTS_FILE_NAME, SECGOV_BASE_DIR

import logging

DATE_FORMAT = '%Y-%m-%d'

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')

@csrf_exempt
def request_facts(request, last_time=None):
    str_body = request.body.decode()
    args = json.loads(str_body)
    ciks = args.get('ciks')
    period = args.get('period') # 'Q', 'S', 'A'
    facts = args.get('facts')
    types = args.get('types') # report types
    skip_segment = args.get('skip_segment', False)
    only_latest = args.get('only_latest', False)
    columns = args.get('columns')

    if ciks is None:
        return HttpResponse("ciks are mandatory", status=400)

    if facts is None:
        return HttpResponse("facts are mandatory", status=400)

    if len(facts) * len(ciks) > 3000:
        return HttpResponse("batch is too big", status=400)

    min_date = args.get('min_date', '2007-01-01')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(2007, 1, 1)

    max_date = args.get('max_date')
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

    result = get_facts(ciks, facts, columns, max_date, min_date, period, skip_segment, types)
    result = list(result)

    if only_latest:
        pass

    resp = json.dumps(result)

    return HttpResponse(resp, content_type='application/json')


def get_facts(ciks=None, facts=None, columns=None, max_date=None, min_date=None, period=None, skip_segment=None, types=None):
    if type(max_date) == datetime.date:
        max_date = str(max_date)
    if type(min_date) == datetime.date:
        min_date = str(min_date)

    min_period=0
    max_period=sys.maxsize
    if period is not None:
        if period == 'A':
            min_period = 360
            max_period = 370
        elif period == 'S':
            min_period = 175
            max_period = 185
        elif period == 'Q':
            min_period = 85
            max_period = 95

    for cik in ciks:
        try:
            with zipfile.ZipFile(os.path.join(SECGOV_BASE_DIR, cik, SEC_GOV_FACTS_FILE_NAME)) as z:
                for fact in facts:
                    try:
                        raw = z.read(fact + '.json')
                        page = json.loads(raw)
                    except KeyError:
                        continue
                    except Exception:
                        logger.exception("unexpected")
                        continue
                    for obj in page:
                        if max_date is not None and max_date < obj['report_date']:
                            continue
                        if min_date is not None and min_date > obj['report_date']:
                            continue
                        if obj['period_length'] is not None and not (min_period <= obj['period_length'] <= max_period):
                            continue
                        if skip_segment and obj['segment'] is not None:
                            continue
                        if types is not None and obj['report_type'] not in types:
                            continue
                        clear_fact(obj, columns)
                        yield obj
        except FileNotFoundError:
            continue
        except Exception:
            logger.exception("unexpected")
            continue

def clear_fact(fact, columns):
    if columns is not None and 'cik' not in columns:
        del fact['cik']

    if columns is not None and 'report_date' not in columns:
        del fact['report_date']
    if columns is not None and 'report_url' not in columns:
        del fact['report_url']
    if columns is not None and 'report_id' not in columns:
        del fact['report_id']
    if columns is not None and 'report_type' not in columns:
        del fact['report_type']

    if columns is not None and 'fact_name' not in columns:
        del fact['fact_name']
    if columns is not None and 'segment' not in columns:
        del fact['segment']

    if columns is not None and 'period' not in columns:
        del fact['period']
    if columns is not None and 'period_type' not in columns:
        del fact['period_type']
    if columns is not None and 'period_length' not in columns:
        del fact['period_length']

    if columns is not None and 'unit' not in columns:
        del fact['unit']
    if columns is not None and 'unit_type' not in columns:
        del fact['unit_type']


def get_facts_(ciks=None, facts=None, columns=None, max_date=None, min_date=None, period=None, skip_segment=None, types=None):
    for r in get_fillings(types, ciks=ciks, facts=facts, skip_segment=skip_segment, min_date=min_date,
                          max_date=max_date, offset=None, limit=None):
        facts = extract_facts(report=r, columns=columns, period=period)
        for f in facts:
            yield f


def extract_facts(report, columns, period):
    if period is not None:
        min_period=0
        max_period=1
        if period == 'A':
            min_period = 360
            max_period = 370
        elif period == 'S':
            min_period = 175
            max_period = 185
        elif period == 'Q':
            min_period = 85
            max_period = 95
    for f in report['facts']:
        if period is not None and f.get('period') is not None and type(f['period'].get('value')) == list:
            start = datetime.datetime.strptime(f['period']['value'][0], DATE_FORMAT).date()
            end = datetime.datetime.strptime(f['period']['value'][1], DATE_FORMAT).date()
            diff = end - start
            if diff.days < min_period or diff.days > max_period:
                continue
        yield format_fact(report, f, columns)


def format_fact(report, fact, columns):
    res = {
        "value": fact['value']
    }

    if columns is None or 'cik' in columns:
        res['cik'] = report['cik']

    if columns is None or 'report_date' in columns:
        res['report_date'] = report['date']
    if columns is None or 'report_url' in columns:
        res['report_url'] = report['url']
    if columns is None or 'report_id' in columns:
        res['report_id'] = report['url'].split('/data/',1)[1].split('/', 2)[1]
    if columns is None or 'report_type' in columns:
        res['report_type'] = report['type']

    if columns is None or 'fact_name' in columns:
        res['fact_name'] = fact['name']
    if columns is None or 'segment' in columns:
        res['segment'] = fact['segment']

    if columns is None or 'period' in columns:
        res['period'] = fact['period']['value']
    if columns is None or 'period_type' in columns:
        res['period_type'] = fact['period']['type']
    if columns is None or 'period_length' in columns:
        if type(fact['period']['value']) == list:
            start = datetime.datetime.strptime(fact['period']['value'][0], DATE_FORMAT).date()
            end = datetime.datetime.strptime(fact['period']['value'][1], DATE_FORMAT).date()
            res['period_length'] = (end - start).days
        else:
            res['period_length'] = None

    if columns is None or 'unit' in columns:
        res['unit'] = fact['unit']['value']
    if columns is None or 'unit_type' in columns:
        res['unit_type'] = fact['unit']['type']

    return res


@csrf_exempt
def get_sec_gov_forms(request, last_time=None):
    str_body = request.body.decode()
    args = json.loads(str_body)

    ciks = args.get('ciks')
    types = args.get('types')
    facts = args.get('facts')
    offset = args.get('offset', 0)
    skip_segment = args.get('skip_segment', False)

    min_date = args.get('min_date')
    min_date = datetime.datetime.strptime(min_date, DATE_FORMAT).date() if min_date is not None else datetime.date(2007, 1, 1)

    max_date = args.get('max_date')
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

    limit = 20
    if facts is not None:
        limit = 20000 // (len(facts) * 10 + 1)

    page = get_fillings(types, ciks, facts, skip_segment, min_date, max_date, offset, limit)
    page = list(page)

    #print(len(page))
    resp = json.dumps(page)
    return HttpResponse(resp, content_type='application/json')


def get_fillings(types=None, ciks=None, facts=None, skip_segment=None, min_date=None, max_date=None, offset=0, limit=1):
    if types is not None:
        types = [t.replace('/', '-') for t in types]

    for r1 in sorted(os.listdir(SECGOV_BASE_DIR)):
        if r1 == SEC_GOV_IDX_FILE_NAME:
            continue
        if ciks is not None and r1 not in ciks:
            continue

        try:
            with gzip.open(os.path.join(SECGOV_BASE_DIR, r1, SEC_GOV_IDX_FILE_NAME), 'rt') as f:
                idx = f.read()
            idx = json.loads(idx)
        except FileNotFoundError:
            continue
        except Exception:
            logger.exception("unexpected")
            continue

        idx = [i for i in idx
                if  (min_date is None or datetime.date.fromisoformat(i['date']) >= min_date)
                and (max_date is None or datetime.date.fromisoformat(i['date']) >= max_date)
               ]

        idx = [i for i in idx if types is None or i['type'] not in types ]

        if offset - len(idx) >= 0:
            offset -= len(idx)
            continue
        else:
            idx = idx[offset:]
            offset = 0

        try:
            with zipfile.ZipFile(os.path.join(SECGOV_BASE_DIR, r1, SEC_GOV_CONTENT_FILE_NAME), 'r') as z:
                for i in idx:
                    if limit <= 0:
                        return
                    limit -= 1
                    try:
                        r = z.read(i['id'] + '.json')
                        r = r.decode()
                        r = json.loads(r)
                    except Exception:
                        logger.exception("unexpected")
                        continue
                    r['facts'] = [ f for f in r['facts']
                                   if (facts is None or f['name'] in facts) and (not skip_segment or f.get('segment') is None)
                                 ]
                    # backward compatibility
                    for f in r['facts']:
                        if 'segment' not in f:
                            f['segment'] = None
                        if 'unit' not in f:
                            f['unit'] = None
                        if 'period' not in f:
                            f['period'] = None

                    yield r

        except FileNotFoundError:
            continue
        except Exception:
            logger.exception("unexpected")
            continue


if __name__ == '__main__':

    # t = time.time()
    # f = get_fillings(
    #     facts=['us-gaap:Assets'],
    #     max_date=datetime.date(2013, 7, 1),
    #     skip_segment=True,
    #     limit=20000
    # )
    # f = list(f)
    # print(len(f))
    # print(time.time()-t)

    t = time.time()
    f = get_facts(facts=['us-gaap:Assets','us-gaap:Liabilities','us-gaap:EarningsPerShareDiluted','us-gaap:AssetsCurrent'],
                  ciks=["796343","883984","901491","1349436","1002638","857737","1357615","29915","1168054","1070412",
                        "92380","914208","1064728","55785","1324404","6769","920148","63276","101829","93410","1065280",
                        "40545","886163","1037676","79282","88205","918160","30625","885639","7332","56873","1037016",
                        "1048286","1000697","922224","101538","107263","769397","732717","68505","1144519","47217","96223",
                        "21665","927066","1166126","949039","1004980","19617","885725","59478","59558","29989","40987","6951",
                        "320193","886982","29669","1365135","882835","27419","936468","874766","79879","812074","858470",
                        "1047122","1031296","4281","1276520","1039684","889900","815097","10795","1326160","34088","277948",
                        "64040","815556","827054","1267238","108772","1156375","315852","1180262","910073","54480","36270",
                        "895126","1043219","8868","104169","1001082","1013871","908255","39911","831259","29905","203527",
                        "101830","101778","718877","1124198","1125259","60667","1296445","1035267","315189","804328",
                        "1326380","843006","721693","1163165","1164727","1437107","12659","1141391","216228","859737",
                        "320187","78003","354950","1053507","1043604","1413329","1378946","315293","1306830","1341439",
                        "310764","72207","1035002","109198","940942","97216","1163302","1022079","877890","1103982","891103",
                        "773840","1466258","4447","32604","66740","52988","1001258","38777","1024478","18230","1060391",
                        "1022646","73124","1048911","823768","1137774","21344","874761","896262","927653","1037868",
                        "927628","36104","75362","1396009","63908","78814","1390777","1032208","80424","80661","1156039",
                        "797468","701985","1410098","1043277","895421","715957","72333","62709","793952","62996","50863",
                        "55067","717423","805676","820027","1173313","1451505","49071","821189","1130310","37996","1058290",
                        "1166691","4962","14272","97745","1281761","1201792","1047862","1058057","67716","31462","46765",
                        "60086","1800","1002910","5513","35527","100885","49826","1090012","1136893","1014473","310158",
                        "47111","1090872","1364742","723531","12927","92230","1458891","1385157","1018963","351817",
                        "65984","39899","1517130","1099219","24741","936340","64803","4977","818479","92122","899051",
                        "820313","97476","915389","1113169","1018724","764180","89800","217346","1487843","1334589",
                        "1050915","10456","37785","1001838","1121788","33213","45012","51434","2969","1403161","1108524",
                        "712515","40533","1355096","702165","798354","4904","836809","833444","1140859","882095","896159",
                        "49938","26172","1136869","1059556","72971","6281","24545","1109357","783325","314808","1133421",
                        "875045","813828","1126328","1090727","1274494","200406","704051","93751","1065088","1045810",
                        "40704","746515","713676","313616","732712","318154","73309","277135","788784","753308",
                        "829224","72903","1021860","849399","731766","886158","827052","70858","931148","1032033",
                        "91576","794367","5272","23217","708819","831001","866374","86312","877212","721371","77476",
                        "1378624","789019","921738","1393612","96021","1014739","21076","861878","1086222","816284","8670",
                        "764065","40211","1038357","1158463","859598","896878","764478","8858","76334","1467373","110621",
                        "866787","16732","320335","1336047","1022671","800240","745732","1056358","354908","1399352",
                        "858877","354707","1273813","106040","1041061","898173","103379","1046311","1163739","909832",
                        "1384135","1116132","945764","750556","1381197","1534675","1418135","91767","1305323","42888",
                        "4127","36047","896493","7084","1006837","31235","1368365","1108967","1421461","918646","794170",
                        "1326200","940944","883241","26324","1141197","1130464","798949","790526","914025","1435064",
                        "912093","880807","726958","773910","705432","1161728","886035","920760","1438533","1437491",
                        "1018840","1339947","789460","63296","230557","814586","807882","728535","49196","944695","64996",
                        "77360","38725","851968","1017480","1405495","1075531","310142","733269","91928","1308547","816761",
                        "225648","720672","750004","839470","1041657","884887","717605","1126956","1273441","99780","933036",
                        "1012100","1389050","84839","720005","947559","215466","1364885","1110783","1111711","1048477",
                        "1046568","8504","725363","780571","1140536","714310","1495932","899866","947484","1219601",
                        "33185","6201","1361658","1173431","1289419","85961","52795","1444363","857949","912728",
                        "936528","1157075","36966","3453","867773","1032975","89089","1224608","876167","1008848",
                        "842023","868278","1015780","875320","353184","1287808","105319","1013880","70145","719413",
                        "776867","917520","801337","894405","1000229","1336917","929008","1106644","1069202","935703",
                        "880266","813672","1021561"],
                  columns=['cik','fact_name','period','report_date','report_id'],
                  skip_segment=True
                  )
    f = list(f)
    print(len(f))
    print(time.time()-t)

    # t1 = time.time()
    # r = os.listdir("/")
    # t2 = time.time()
    # r = glob.glob("/*")
    # t3 = time.time()
    #
    # print(t2-t1, t3-t2)

