from django.core.management import BaseCommand

import listing.sync

class Command(BaseCommand):
    def handle(self, **options):
        listing.sync.sync()