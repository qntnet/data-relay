import os

AVANTAGE_KEY = None
SYMBOLS = None
INDEXES = None

MASTER_ADDR = os.environ.get("MASTER_ADDR")
RELAY_KEY = os.environ.get("REPLICATION_KEY")
NODE_NAME = os.environ.get("NODE_NAME", None)
