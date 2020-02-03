import urllib.request
import urllib.error
import traceback
import time
import logging

logger = logging.getLogger(__name__)
ERROR_DELAY = 5


def load_with_retry(url):
    while True:
        try:
            logger.info("request " + url)
            response = urllib.request.urlopen(url, timeout=20)
            raw = response.read()
            return raw
        except KeyboardInterrupt:
            raise
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return None
        except:
            logging.exception("request exception")
            time.sleep(ERROR_DELAY)
