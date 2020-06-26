from django.core.management import BaseCommand

import assets.sync
import secgov.sync
import idx.sync
from datarelay.http import request_with_retry
from replication.conf import STOCKS_LAST_DATE_FILE_NAME, STOCKS_LAST_DATE_URL, POST_STATUS_URL


class Command(BaseCommand):
    def handle(self, **options):
        dt = None
        try:
            with open(STOCKS_LAST_DATE_FILE_NAME, 'r') as f:
                dt = f.read()
        except:
            pass

        server_dt = request_with_retry(STOCKS_LAST_DATE_URL).decode()

        if dt is None or server_dt > dt:

            assets.sync.sync_list()
            assets.sync.sync_data()
            #secgov.sync.sync()
            idx.sync.sync_list()
            idx.sync.sync_series()

            request_with_retry(POST_STATUS_URL + "/stocks/" + server_dt + "/")

            with open(STOCKS_LAST_DATE_FILE_NAME, 'w') as f:
                f.write(server_dt)