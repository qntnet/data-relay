from django.core.management import BaseCommand

import crypto.sync
from datarelay.http import request_with_retry
from replication.conf import CRYPTO_LAST_DATE_FILE_NAME, CRYPTO_LAST_DATE_URL, POST_STATUS_URL, \
    STOCKS_LAST_DATE_FILE_NAME


class Command(BaseCommand):
    def handle(self, **options):
        dt = ''
        try:
            with open(CRYPTO_LAST_DATE_FILE_NAME, 'r') as f:
                dt = f.read()
        except:
            pass
        request_with_retry(POST_STATUS_URL + "/crypto/" + dt + "/")


        dt = ''
        try:
            with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
                dt = f.read()
        except:
            pass
        request_with_retry(POST_STATUS_URL + "/stocks/" + dt + "/")