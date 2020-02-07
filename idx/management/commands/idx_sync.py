from django.core.management import BaseCommand

import idx.sync

class Command(BaseCommand):
    def handle(self, **options):
        idx.sync.sync_list()
        idx.sync.sync_series()