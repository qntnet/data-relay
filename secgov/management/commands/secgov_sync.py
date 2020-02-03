from django.core.management import BaseCommand

import secgov.sync

class Command(BaseCommand):
    def handle(self, **options):
        secgov.sync.sync()