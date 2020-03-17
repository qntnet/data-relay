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
            response = urllib.request.urlopen(url, data=data, timeout=MASTER_TIMEOUT)
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
