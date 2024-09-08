from dataclasses import dataclass

import pytest

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.types import NodeType


@dataclass
class BaseResourceWithDefaultField(BaseResource):
    field_with_default: bool = True


class TestMinorSchemaChange:
    @pytest.fixture
    def base_resource(self):
        return BaseResource(
            name="test",
            resource_type=NodeType.Model,
            package_name="test_package",
            path="test_path",
            original_file_path="test_original_file_path",
            unique_id="test_unique_id",
        )

    @pytest.fixture
    def base_resource_new_default_field(self):
        return BaseResourceWithDefaultField(
            name="test",
            resource_type=NodeType.Model,
            package_name="test_package",
            path="test_path",
            original_file_path="test_original_file_path",
            unique_id="test_unique_id",
            field_with_default=False,
        )

    def test_serializing_new_default_field_is_backward_compatabile(
        self, base_resource_new_default_field
    ):
        # old code (using old class) can create an instance of itself given new data (new class)
        BaseResource.from_dict(base_resource_new_default_field.to_dict())

    def test_serializing_new_default_field_is_forward_compatible(self, base_resource):
        # new code (using new class) can create an instance of itself given old data (old class)
        BaseResourceWithDefaultField.from_dict(base_resource.to_dict())

    def test_serializing_removed_default_field_is_backward_compatabile(self, base_resource):
        # old code (using old class with default field) can create an instance of itself given new data (class w/o default field)
        old_resource = BaseResourceWithDefaultField.from_dict(base_resource.to_dict())
        # set to the default value when not provided in data
        assert old_resource.field_with_default is True

    def test_serializing_removed_default_field_is_forward_compatible(
        self, base_resource_new_default_field
    ):
        # new code (using class without default field) can create an instance of itself given old data (class with old field)
        BaseResource.from_dict(base_resource_new_default_field.to_dict())
