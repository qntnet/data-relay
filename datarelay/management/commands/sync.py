from django.core.management import BaseCommand

import idx.sync
import assets.sync
import secgov.sync

class Command(BaseCommand):
    def handle(self, **options):
        assets.sync.sync_list()
        assets.sync.sync_data()
        secgov.sync.sync()
        idx.sync.sync_list()
        idx.sync.sync_series()