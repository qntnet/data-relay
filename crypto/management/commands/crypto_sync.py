from django.core.management import BaseCommand

import crypto.sync

class Command(BaseCommand):
    def handle(self, **options):
        crypto.sync.sync()