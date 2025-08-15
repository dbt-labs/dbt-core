from argparse import Namespace
from unittest.mock import patch

from dbt.flags import get_flags, set_from_args
from dbt.task.list import ListTask
from dbt_common.events.types import PrintEvent


def test_list_output_results():
    set_from_args(Namespace(models=None), {})
    task = ListTask(get_flags(), None, None)
    results = ["node1", "node2", "node3"]
    expected_node_results = ["node1", "node2", "node3"]

    with patch("dbt.task.list.fire_event") as mock_fire_event:
        node_results = task.output_results(results)

    assert node_results == expected_node_results
    # assert called with PrintEvent type object and message 'node1', 'node2', 'node3'
    for call_args in mock_fire_event.call_args_list:
        assert isinstance(call_args[0][0], PrintEvent)
        assert call_args[0][0].msg in expected_node_results


class TestGetNestedValue:
    """Unit tests for the _get_nested_value method"""
    
    def setup_method(self):
        set_from_args(Namespace(models=None), {})
        self.task = ListTask(get_flags(), None, None)

    def test_get_nested_value_simple_key(self):
        """Test getting a simple top-level key"""
        data = {"name": "test_model", "type": "model"}
        result = self.task._get_nested_value(data, "name")
        assert result == "test_model"

    def test_get_nested_value_nested_key(self):
        """Test getting a nested key"""
        data = {
            "name": "test_model",
            "config": {
                "materialized": "table",
                "tags": ["important"]
            }
        }
        result = self.task._get_nested_value(data, "config.materialized")
        assert result == "table"

    def test_get_nested_value_deep_nested_key(self):
        """Test getting a deeply nested key"""
        data = {
            "name": "test_model",
            "config": {
                "meta": {
                    "owner": "data-team",
                    "contact": {
                        "email": "team@company.com"
                    }
                }
            }
        }
        result = self.task._get_nested_value(data, "config.meta.owner")
        assert result == "data-team"
        
        result = self.task._get_nested_value(data, "config.meta.contact.email")
        assert result == "team@company.com"

    def test_get_nested_value_nonexistent_top_level(self):
        """Test getting a non-existent top-level key returns None"""
        data = {"name": "test_model", "type": "model"}
        result = self.task._get_nested_value(data, "nonexistent")
        assert result is None

    def test_get_nested_value_nonexistent_nested(self):
        """Test getting a non-existent nested key returns None"""
        data = {
            "name": "test_model",
            "config": {
                "materialized": "table"
            }
        }
        result = self.task._get_nested_value(data, "config.nonexistent")
        assert result is None

    def test_get_nested_value_nonexistent_parent(self):
        """Test getting a nested key where parent doesn't exist returns None"""
        data = {"name": "test_model", "type": "model"}
        result = self.task._get_nested_value(data, "nonexistent.child")
        assert result is None

    def test_get_nested_value_non_dict_parent(self):
        """Test getting a nested key where parent is not a dict returns None"""
        data = {
            "name": "test_model",
            "config": "not_a_dict"
        }
        result = self.task._get_nested_value(data, "config.materialized")
        assert result is None

    def test_get_nested_value_none_parent(self):
        """Test getting a nested key where parent is None returns None"""
        data = {
            "name": "test_model",
            "config": None
        }
        result = self.task._get_nested_value(data, "config.materialized")
        assert result is None

    def test_get_nested_value_list_value(self):
        """Test getting a nested key that contains a list"""
        data = {
            "name": "test_model",
            "config": {
                "tags": ["important", "daily"]
            }
        }
        result = self.task._get_nested_value(data, "config.tags")
        assert result == ["important", "daily"]

    def test_get_nested_value_dict_value(self):
        """Test getting a nested key that contains a dict"""
        data = {
            "name": "test_model",
            "config": {
                "meta": {"owner": "data-team", "criticality": "high"}
            }
        }
        result = self.task._get_nested_value(data, "config.meta")
        assert result == {"owner": "data-team", "criticality": "high"}


class TestGenerateJson:
    """Unit tests for the generate_json method to ensure coverage of nested key logic"""

    def setup_method(self):
        set_from_args(Namespace(models=None, output_keys=None), {})
        self.task = ListTask(get_flags(), None, None)

    def create_mock_node(self, name="test_model", materialized="table", meta=None):
        """Helper to create a mock node with specified properties"""
        from unittest.mock import Mock
        
        node = Mock()
        node.to_dict.return_value = {
            "name": name,
            "resource_type": "model",
            "unique_id": f"model.test.{name}",
            "config": {
                "materialized": materialized,
                "tags": [],
                "meta": meta or {},
            },
            "original_file_path": f"models/{name}.sql",
        }
        return node

    def test_generate_json_with_nested_keys(self):
        """Test generate_json with nested output keys"""
        # Mock args to have nested output keys
        with patch.object(self.task, 'args') as mock_args:
            mock_args.output_keys = ["name", "config.materialized"]
            
            # Mock _iterate_selected_nodes to return our test node
            with patch.object(self.task, '_iterate_selected_nodes') as mock_iterate:
                node = self.create_mock_node("test_model", "table")
                mock_iterate.return_value = [node]
                
                # Get the result
                results = list(self.task.generate_json())
                
                assert len(results) == 1
                import json
                result_data = json.loads(results[0])
                assert result_data["name"] == "test_model"
                assert result_data["config.materialized"] == "table"
            
    def test_generate_json_with_nonexistent_nested_keys(self):
        """Test generate_json with non-existent nested keys"""
        # Mock args to have non-existent nested key
        with patch.object(self.task, 'args') as mock_args:
            mock_args.output_keys = ["name", "config.nonexistent"]
            
            # Mock _iterate_selected_nodes to return our test node
            with patch.object(self.task, '_iterate_selected_nodes') as mock_iterate:
                node = self.create_mock_node("test_model", "table")
                mock_iterate.return_value = [node]
                
                # Get the result
                results = list(self.task.generate_json())
                
                assert len(results) == 1
                import json
                result_data = json.loads(results[0])
                assert result_data["name"] == "test_model"
                # Non-existent key should not be in result
                assert "config.nonexistent" not in result_data

    def test_generate_json_with_mixed_keys(self):
        """Test generate_json with mix of regular and nested keys"""
        # Mock args to have mixed keys
        with patch.object(self.task, 'args') as mock_args:
            mock_args.output_keys = ["name", "resource_type", "config.materialized", "config.meta.owner"]
            
            # Mock _iterate_selected_nodes to return our test node
            with patch.object(self.task, '_iterate_selected_nodes') as mock_iterate:
                node = self.create_mock_node("test_model", "incremental", {"owner": "data-team"})
                mock_iterate.return_value = [node]
                
                # Get the result
                results = list(self.task.generate_json())
                
                assert len(results) == 1
                import json
                result_data = json.loads(results[0])
                assert result_data["name"] == "test_model"
                assert result_data["resource_type"] == "model"
                assert result_data["config.materialized"] == "incremental"
                assert result_data["config.meta.owner"] == "data-team"

    def test_generate_json_with_no_output_keys(self):
        """Test generate_json without output_keys (default behavior)"""
        # Mock args to have None output_keys
        with patch.object(self.task, 'args') as mock_args:
            mock_args.output_keys = None
            
            # Mock _iterate_selected_nodes to return our test node
            with patch.object(self.task, '_iterate_selected_nodes') as mock_iterate:
                node = self.create_mock_node("test_model", "table")
                mock_iterate.return_value = [node]
                
                # Get the result
                results = list(self.task.generate_json())
                
                assert len(results) == 1
                import json
                result_data = json.loads(results[0])
                # Should contain ALLOWED_KEYS
                for allowed_key in self.task.ALLOWED_KEYS:
                    if allowed_key in node.to_dict.return_value:
                        assert allowed_key in result_data

    def test_generate_json_deep_nested_path(self):
        """Test generate_json with deeply nested paths"""
        # Mock args to have deep nested key
        with patch.object(self.task, 'args') as mock_args:
            mock_args.output_keys = ["config.meta.contact.email"]
            
            # Mock _iterate_selected_nodes to return our test node
            with patch.object(self.task, '_iterate_selected_nodes') as mock_iterate:
                node = self.create_mock_node("test_model", "table", {
                    "contact": {"email": "team@company.com"}
                })
                mock_iterate.return_value = [node]
                
                # Get the result
                results = list(self.task.generate_json())
                
                assert len(results) == 1
                import json
                result_data = json.loads(results[0])
                assert result_data["config.meta.contact.email"] == "team@company.com"
