from django.core.management import BaseCommand

import assets.sync

class Command(BaseCommand):
    def handle(self, **options):
        assets.sync.sync_list()
        assets.sync.sync_data()