import os

AVANTAGE_KEY = None
SYMBOLS = None
INDEXES = None
BLSGOV_DBS = None

MASTER_ADDR = os.environ.get("MASTER_ADDR", "")
RELAY_KEY = os.environ.get("REPLICATION_KEY", "")
NODE_NAME = os.environ.get("NODE_NAME", "")

MASTER_TIMEOUT = int(os.environ.get("TIMEOUT", "5"))
MASTER_ERROR_DELAY = int(os.environ.get("DELAY", "5"))

SECGOV_INCREMENTAL_UPDATE = True