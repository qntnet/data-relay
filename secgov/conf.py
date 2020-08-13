import os
from urllib.parse import urljoin
from datarelay.settings import MASTER_ADDR, WORK_DIR

SECGOV_BASE_DIR = os.path.join(WORK_DIR, 'secgov4')

SEC_GOV_CONTENT_FILE_NAME = "content.zip"
SEC_GOV_FACTS_FILE_NAME = "facts.zip"
SEC_GOV_IDX_FILE_NAME = "index.json.gz"

BASE_URL =  urljoin(MASTER_ADDR, '/sec.gov/api/files/')