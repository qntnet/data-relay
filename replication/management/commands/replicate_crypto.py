from django.core.management import BaseCommand

import crypto.sync
from datarelay.http import request_with_retry
from replication.conf import CRYPTO_LAST_DATE_FILE_NAME, CRYPTO_LAST_DATE_URL, POST_STATUS_URL


class Command(BaseCommand):
    def handle(self, **options):
        dt = None
        try:
            with open(CRYPTO_LAST_DATE_FILE_NAME, 'r') as f:
                dt = f.read()
        except:
            pass

        server_dt = request_with_retry(CRYPTO_LAST_DATE_URL).decode()

        if dt is None or server_dt > dt:

            crypto.sync.sync()

            request_with_retry(POST_STATUS_URL + "/crypto/" + server_dt + "/")

            with open(CRYPTO_LAST_DATE_FILE_NAME, 'w') as f:
                f.write(server_dt)
