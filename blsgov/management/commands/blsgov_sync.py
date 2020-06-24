from django.core.management import BaseCommand

import blsgov.sync

class Command(BaseCommand):
    def handle(self, **options):
        blsgov.sync.sync_dbs()