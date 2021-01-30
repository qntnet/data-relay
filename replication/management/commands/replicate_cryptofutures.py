from django.core.management import BaseCommand

import cryptofutures.sync
from datarelay.http import request_with_retry
from replication.conf import POST_STATUS_URL, CRYPTOFUTURES_LAST_DATE_FILE_NAME, CRYPTOFUTURES_LAST_DATE_URL


class Command(BaseCommand):
    def handle(self, **options):
        dt = None
        try:
            with open(CRYPTOFUTURES_LAST_DATE_FILE_NAME, 'r') as f:
                dt = f.read()
            request_with_retry(POST_STATUS_URL + "/cryptofutures/" + dt + "/")
        except:
            pass

        server_dt = request_with_retry(CRYPTOFUTURES_LAST_DATE_URL).decode()

        if dt is None or server_dt > dt:

            cryptofutures.sync.sync()

            request_with_retry(POST_STATUS_URL + "/cryptofutures/" + server_dt + "/")

            with open(CRYPTOFUTURES_LAST_DATE_FILE_NAME, 'w') as f:
                f.write(server_dt)
