import os
import sys
from dbt.events.types import BuildingCatalog
from dbt.events import proto_types as pl
from dbt.events.functions import event_to_dict



class TestExtraEventDict:
    def test_extra_dict_on_event(self):
        os.environ["DBT_ENV_CUSTOM_ENV_env_key"] = "env_value"

        event = BuildingCatalog()
        event_dict = event_to_dict(event)
        serialized = bytes(event)
        assert "extra" in event_dict["info"].keys()
        assert event.info.extra == {"env_key": "env_value"}

        # Extract EventInfo from serialized message
        generic_event = pl.GenericMessage().parse(serialized)
        assert generic_event.info.code == "E044"
        # get the message class for the real message from the generic message
        message_class = getattr(sys.modules["dbt.events.proto_types"], generic_event.info.name)
        new_event = message_class().parse(serialized)
        assert new_event.info.extra == event.info.extra

        #cleanup
        del os.environ["DBT_ENV_CUSTOM_ENV_env_key"]
