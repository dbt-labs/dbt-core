import subprocess

from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions

NOT_INSTALLED_MSG = """
dbt requires the gcloud SDK to be installed to authenticate with BigQuery.
Please download and install the SDK, or use a Service Account instead.

https://cloud.google.com/sdk/
"""


def log_cmd(cmd):
    logger.debug('Executing "{}"'.format(' '.join(cmd)))


def run_cmd(cwd, cmd):
    log_cmd(cmd)
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    return proc.communicate()


def gcloud_installed():
    try:
        run_cmd('.', ['gcloud', '--version'])
        return True
    except OSError as e:
        logger.debug(e)
        return False


def setup_default_credentials():
    if gcloud_installed():
        run_cmd('.', ["gcloud", "auth", "application-default", "login"])
    else:
        raise dbt.exceptions.RuntimeException(NOT_INSTALLED_MSG)
