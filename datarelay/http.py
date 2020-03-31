import urllib.request
import urllib.error
import time
import logging
import socket

from datarelay.settings import MASTER_TIMEOUT, MASTER_ERROR_DELAY

logger = logging.getLogger(__name__)


def request_with_retry(url, data = None):
    while True:
        try:
            logger.info("request " + url)
            response = urlopen(url, data=data, timeout=MASTER_TIMEOUT)
            if response.status == 404:
                return None
            raw = response.read()
            return raw
        except KeyboardInterrupt:
            raise
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return None
            else:
                logger.exception("wrong status code")
                time.sleep(MASTER_ERROR_DELAY)
        except socket.timeout:
            logging.warning("timeout")
            time.sleep(MASTER_ERROR_DELAY)
        except:
            logging.exception("request exception")
            time.sleep(MASTER_ERROR_DELAY)



from http.client import HTTPConnection, HTTPSConnection, ImproperConnectionState

import select
connections = {}


def request(method, url, body=None, headers={}, **kwargs):
    scheme, _, host, path = url.split('/', 3)
    h = connections.get((scheme, host))
    if h:
        select_result = True
        try:
            select_result = select.select([h.sock], [], [], 0)[0]
        except:
            pass
        if not select_result:
            try:
                h.close()
            except:
                pass
            h = None
    if not h:
        Connection = HTTPConnection if scheme == 'http:' else HTTPSConnection
        h = connections[(scheme, host)] = Connection(host, **kwargs)
    try:
        h.request(method, '/' + path, body, headers)
        return h.getresponse()
    except ImproperConnectionState as err:
        h.close()
        connections[(scheme, host)] = None
        raise err


def urlopen(url, data=None, *args, **kwargs):
    resp = request('POST' if data else 'GET', url, data, *args, **kwargs)
    assert resp.status < 500, (resp.status, resp.reason, resp.read())
    return resp
