from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from mashumaro.mixins.msgpack import DataClassMessagePackMixin


@dataclass
class ExternalPartition(DataClassMessagePackMixin):
    name: str = ""
    description: str = ""
    data_type: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExternalTable(DataClassMessagePackMixin):
    location: Optional[str] = None
    file_format: Optional[str] = None
    row_format: Optional[str] = None
    tbl_properties: Optional[str] = None
    partitions: Optional[Union[List[ExternalPartition], List[str]]] = None


def test_partitions_serialization():

    part1 = ExternalPartition(
        name="partition 1",
        description="partition 1",
        data_type="string",
    )

    part2 = ExternalPartition(
        name="partition 2",
        description="partition 2",
        data_type="string",
    )

    ext_table = ExternalTable(
        location="my_location",
        file_format="my file format",
        row_format="row format",
        partitions=[part1, part2],
    )

    ext_table_dict = ext_table.to_dict()
    assert isinstance(ext_table_dict["partitions"][0], dict)

    ext_table_msgpack = ext_table.to_msgpack()
    assert ext_table_msgpack
