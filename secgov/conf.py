import os
from urllib.parse import urljoin
from datarelay.settings import MASTER_ADDR, WORK_DIR

SECGOV_BASE_DIR = os.path.join(WORK_DIR, 'sec.gov')
SEC_GOV_LAST_ID_FILE_NAME = os.path.join(SECGOV_BASE_DIR, "last_id.txt")
SECGOV_FORMS_DIR_NAME = os.path.join(SECGOV_BASE_DIR, "forms")

BASE_URL =  urljoin(MASTER_ADDR, 'master/sec.gov/forms')