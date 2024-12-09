from dbt.artifacts.resources import ExternalPartition, ExternalTable


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
    ext_table.validate(ext_table_dict)
