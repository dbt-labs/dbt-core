#!/bin/bash
# Compare wheel contents between setuptools and hatch builds

set -e

BASELINE=$1
HATCH=$2

if [ -z "$BASELINE" ] || [ -z "$HATCH" ]; then
    echo "Usage: $0 <baseline-dir> <hatch-dir>"
    exit 1
fi

echo "=== File List Comparison ==="
echo "Comparing file lists..."
DIFF_OUTPUT=$(diff <(cd "$BASELINE" && find . -type f | sort) \
     <(cd "$HATCH" && find . -type f | sort) || true)

if [ -z "$DIFF_OUTPUT" ]; then
    echo "✓ File lists are identical"
else
    echo "✗ File lists differ:"
    echo "$DIFF_OUTPUT"
fi

echo ""
echo "=== File Count ==="
BASELINE_COUNT=$(cd "$BASELINE" && find . -type f | wc -l | tr -d ' ')
HATCH_COUNT=$(cd "$HATCH" && find . -type f | wc -l | tr -d ' ')
echo "Baseline: $BASELINE_COUNT files"
echo "Hatch: $HATCH_COUNT files"

if [ "$BASELINE_COUNT" = "$HATCH_COUNT" ]; then
    echo "✓ File counts match"
else
    echo "✗ File counts differ"
fi

echo ""
echo "=== Metadata Comparison ==="
METADATA_DIFF=$(diff <(cat "$BASELINE"/dbt_core-*.dist-info/METADATA | grep -v "^Generator:") \
     <(cat "$HATCH"/dbt_core-*.dist-info/METADATA | grep -v "^Generator:") || true)

if [ -z "$METADATA_DIFF" ]; then
    echo "✓ Metadata is identical (ignoring Generator line)"
else
    echo "✗ Metadata differs:"
    echo "$METADATA_DIFF"
fi

echo ""
echo "=== Entry Points ==="
ENTRY_DIFF=$(diff <(cat "$BASELINE"/dbt_core-*.dist-info/entry_points.txt) \
     <(cat "$HATCH"/dbt_core-*.dist-info/entry_points.txt) || true)

if [ -z "$ENTRY_DIFF" ]; then
    echo "✓ Entry points are identical"
else
    echo "✗ Entry points differ:"
    echo "$ENTRY_DIFF"
fi

echo ""
echo "=== Package Contents (from RECORD) ==="
echo "Comparing package contents..."
RECORD_DIFF=$(diff <(cat "$BASELINE"/dbt_core-*.dist-info/RECORD | cut -d',' -f1 | sort) \
     <(cat "$HATCH"/dbt_core-*.dist-info/RECORD | cut -d',' -f1 | sort) || true)

if [ -z "$RECORD_DIFF" ]; then
    echo "✓ Package contents are identical"
else
    echo "✗ Package contents differ:"
    echo "$RECORD_DIFF"
fi

echo ""
echo "=== Summary ==="
if [ -z "$DIFF_OUTPUT" ] && [ -z "$METADATA_DIFF" ] && [ -z "$ENTRY_DIFF" ] && [ -z "$RECORD_DIFF" ]; then
    echo "✓✓✓ All checks passed! Wheels are equivalent."
    exit 0
else
    echo "✗✗✗ Some differences found. Review output above."
    exit 1
fi
