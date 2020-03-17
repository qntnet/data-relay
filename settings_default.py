import os

AVANTAGE_KEY = "real_key"
SYMBOLS = ['NASDAQ:AAPL', 'NASDAQ:FB'] # None for all
INDEXES = ['SPX'] # None for all

MASTER_TIMEOUT = 12
MASTER_ERROR_DELAY = 12

AVANTAGE_DELAY = 12
AVANTAGE_TIMEOUT = 12
AVANTAGE_RATE_LIMIT_DELAY = 60

MASTER_ADDR = "https://data-master.quantnet.ai"
WORK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "work")

RELAY_KEY = None
