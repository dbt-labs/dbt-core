#!/usr/bin/env python3
"""Simple test to verify the suffix generation logic."""

from datetime import datetime

def get_test_table_suffix(store_failures_unique, store_failures_suffix=None):
    """Generate a unique suffix for test failure tables based on configuration."""
    if not store_failures_unique:
        return None
        
    suffix_strategy = store_failures_suffix or 'invocation_id'
    
    if suffix_strategy == 'invocation_id':
        # Use first 8 chars of invocation_id for reasonable table name length
        # Mock invocation_id for test
        invocation_id = "abcd1234-5678-90ef-ghij-klmnopqrstuv"
        return invocation_id[:8] if invocation_id else None
        
    elif suffix_strategy == 'timestamp':
        # Full timestamp: YYYYMMDD_HHMMSS
        return datetime.now().strftime('%Y%m%d_%H%M%S')
        
    elif suffix_strategy == 'date':
        # Date only: YYYYMMDD
        return datetime.now().strftime('%Y%m%d')
        
    elif suffix_strategy == 'hour':
        # Date and hour: YYYYMMDD_HH - useful for hourly DAGs
        return datetime.now().strftime('%Y%m%d_%H')
        
    else:
        # Treat as literal string - could be expanded to support templates
        return suffix_strategy

def test_suffix_generation():
    """Test that suffix generation works correctly."""
    
    test_cases = [
        # (store_failures_unique, store_failures_suffix, expected_check)
        (False, None, lambda x: x is None),  # No suffix
        (True, None, lambda x: x == "abcd1234"),  # Default to invocation_id
        (True, 'invocation_id', lambda x: x == "abcd1234"),  # Invocation ID suffix
        (True, 'timestamp', lambda x: x and len(x) == 15 and x[8] == '_'),  # Timestamp suffix
        (True, 'date', lambda x: x and len(x) == 8 and x.isdigit()),  # Date suffix
        (True, 'hour', lambda x: x and len(x) == 11 and x[8] == '_'),  # Hour suffix
        (True, 'custom_suffix', lambda x: x == 'custom_suffix'),  # Custom literal suffix
        (True, '2024_01_15', lambda x: x == '2024_01_15'),  # Custom date-like suffix
    ]
    
    for store_unique, suffix_strategy, check_func in test_cases:
        suffix = get_test_table_suffix(store_unique, suffix_strategy)
        
        if check_func(suffix):
            print(f"✓ Test passed: store_unique={store_unique}, suffix={suffix_strategy}, result={suffix}")
        else:
            print(f"✗ Test failed: store_unique={store_unique}, suffix={suffix_strategy}, result={suffix}")
            return False
    
    return True

if __name__ == "__main__":
    print("Testing test suffix generation logic...")
    if test_suffix_generation():
        print("\nAll tests passed!")
    else:
        print("\nSome tests failed!")
        sys.exit(1)