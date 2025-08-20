#!/usr/bin/env python3
"""Test script to verify test suffix functionality."""

import os
import sys
from pathlib import Path

# Add dbt-core to path
sys.path.insert(0, str(Path(__file__).parent / "core"))

from dbt.artifacts.resources.v1.config import TestConfig
from dbt.contracts.graph.nodes import GenericTestNode
from dbt.compilation import Compiler
from dbt_common.invocation import get_invocation_id
from unittest.mock import MagicMock, patch

def test_suffix_generation():
    """Test that suffix generation works correctly."""
    
    # Create a mock config
    mock_config = MagicMock()
    compiler = Compiler(mock_config)
    
    # Test various suffix strategies
    test_cases = [
        # (store_failures_unique, store_failures_suffix, expected_pattern)
        (False, None, None),  # No suffix
        (True, 'invocation_id', 'starts_with_hex'),  # Invocation ID suffix
        (True, 'timestamp', 'YYYYMMDD_HHMMSS'),  # Timestamp suffix
        (True, 'date', 'YYYYMMDD'),  # Date suffix
        (True, 'hour', 'YYYYMMDD_HH'),  # Hour suffix
        (True, 'custom_suffix', 'custom_suffix'),  # Custom literal suffix
    ]
    
    for store_unique, suffix_strategy, expected in test_cases:
        # Create a test node
        test_node = GenericTestNode(
            unique_id="test.myproject.test_name",
            name="test_name",
            database="test_db",
            schema="test_schema",
            alias="test_name",
            resource_type="test",
            package_name="myproject",
            path="test.sql",
            original_file_path="test.sql",
            config=TestConfig(
                store_failures=True,
                store_failures_unique=store_unique,
                store_failures_suffix=suffix_strategy
            ),
            fqn=["myproject", "test_name"],
            checksum={"name": "sha256", "checksum": "abc123"}
        )
        
        # Get the suffix
        suffix = compiler._get_test_table_suffix(test_node)
        
        # Verify expectations
        if expected is None:
            assert suffix is None, f"Expected no suffix for {suffix_strategy}"
        elif expected == 'starts_with_hex':
            assert suffix is not None and len(suffix) == 8, f"Expected 8-char invocation ID suffix, got {suffix}"
        elif expected == 'YYYYMMDD_HHMMSS':
            assert suffix is not None and len(suffix) == 15 and suffix[8] == '_', f"Expected timestamp format, got {suffix}"
        elif expected == 'YYYYMMDD':
            assert suffix is not None and len(suffix) == 8, f"Expected date format, got {suffix}"
        elif expected == 'YYYYMMDD_HH':
            assert suffix is not None and len(suffix) == 11 and suffix[8] == '_', f"Expected hour format, got {suffix}"
        else:
            assert suffix == expected, f"Expected {expected}, got {suffix}"
        
        print(f"✓ Test passed: store_unique={store_unique}, suffix={suffix_strategy}, result={suffix}")

if __name__ == "__main__":
    print("Testing test suffix generation...")
    test_suffix_generation()
    print("\nAll tests passed!")