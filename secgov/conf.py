import os
from urllib.parse import urljoin
from datarelay.settings import MASTER_ADDR, WORK_DIR

BASE_DIR = os.path.join(WORK_DIR, 'sec.gov')
LAST_ID_FILE_NAME = os.path.join(BASE_DIR, "last_id.txt")
FORMS_DIR_NAME = os.path.join(BASE_DIR, "forms")

BASE_URL =  urljoin(MASTER_ADDR, 'master/sec.gov/forms')