import os

PACKAGE_PATH = os.path.dirname(__file__)
PROJECT_NAME = 'dbt'

DOCS_INDEX_FILE_PATH = os.path.normpath(
    os.path.join(PACKAGE_PATH, '..', "index.html"))


# Adapter registration will add to this
PACKAGES = {PROJECT_NAME: PACKAGE_PATH}
