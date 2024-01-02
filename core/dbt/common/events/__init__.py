from dbt.common.events.base_types import EventLevel
from dbt.common.events.event_manager_client import get_event_manager
from dbt.common.events.functions import get_stdout_config
from dbt.common.events.logger import LineFormat

# make sure event manager starts with a logger
get_event_manager().add_logger(
    get_stdout_config(LineFormat.PlainText, True, EventLevel.INFO, False)
)
