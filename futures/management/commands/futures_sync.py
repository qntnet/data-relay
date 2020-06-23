from django.core.management import BaseCommand

import futures.sync

class Command(BaseCommand):
    def handle(self, **options):
        futures.sync.sync_list()
        futures.sync.sync_data()