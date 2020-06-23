from django.core.management import BaseCommand

import futures.sync
from datarelay.http import request_with_retry
from replication.conf import POST_STATUS_URL, FUTURES_LAST_DATE_FILE_NAME, FUTURES_LAST_DATE_URL


class Command(BaseCommand):
    def handle(self, **options):
        dt = None
        try:
            with open(FUTURES_LAST_DATE_FILE_NAME, 'r') as f:
                dt = f.read()
        except:
            pass

        server_dt = request_with_retry(FUTURES_LAST_DATE_URL).decode()

        if dt is None or server_dt > dt:
            futures.sync.sync_list()
            futures.sync.sync_data()

            request_with_retry(POST_STATUS_URL + "/futures/" + server_dt + "/")

            with open(FUTURES_LAST_DATE_FILE_NAME, 'w') as f:
                f.write(server_dt)
