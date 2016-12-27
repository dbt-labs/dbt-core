import logging
import sys

# disable logs from other modules, excepting ERROR logs
logging.getLogger('contracts').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


# create a global console logger for dbt
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(message)s'))

logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def initialize_logger(debug_mode=False,):
    if debug_mode:
        handler.setFormatter(logging.Formatter('%(asctime)-18s: %(message)s'))
        logger.setLevel(logging.DEBUG)

GLOBAL_LOGGER = logger
