import os

AVANTAGE_KEY = "real_key"
SYMBOLS = ['NASDAQ:AAPL', 'NASDAQ:FB'] # None for all
INDEXES = ['SPX'] # None for all

MASTER_ADDR = "https://data-master.quantnet.ai"
WORK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "work")