
import os

GLOBAL_DBT_MODULES_PATH = os.path.dirname(__file__)
GLOBAL_PROJECT_NAME = 'dbt'

DOCS_INDEX_FILE_PATH = os.path.normpath(
    os.path.join(GLOBAL_DBT_MODULES_PATH, "index.html"))
