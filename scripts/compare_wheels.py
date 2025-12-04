#!/usr/bin/env python3
"""
Compare two wheel files to verify build artifact equivalence.

This script compares wheels built with different build systems (e.g., hatch vs setuptools)
to ensure the build artifacts are functionally equivalent, while ignoring expected
differences like timestamps and build tool signatures.

Usage:
    python compare_wheels.py <wheel1.whl> <wheel2.whl>

Example:
    python compare_wheels.py dist/dbt_core-1.0.0-py3-none-any.whl dist_hatch/dbt_core-1.0.0-py3-none-any.whl
"""

import argparse
import difflib
import hashlib
import sys
import tempfile
import zipfile
from pathlib import Path


# ANSI color codes for terminal output
class Colors:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_header(text: str) -> None:
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")


def print_success(text: str) -> None:
    print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")


def print_warning(text: str) -> None:
    print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")


def print_error(text: str) -> None:
    print(f"{Colors.RED}✗ {text}{Colors.RESET}")


def print_info(text: str) -> None:
    print(f"{Colors.CYAN}ℹ {text}{Colors.RESET}")


def file_hash(content: bytes) -> str:
    """Calculate SHA256 hash of content."""
    return hashlib.sha256(content).hexdigest()


def normalize_record_file(content: str) -> set[str]:
    """
    Normalize RECORD file for comparison.

    RECORD files contain hashes and sizes that may differ between builds.
    We extract just the filenames for comparison.
    """
    files = set()
    for line in content.strip().split("\n"):
        if line:
            # RECORD format: filename,hash,size
            filename = line.split(",")[0]
            files.add(filename)
    return files


def normalize_wheel_file(content: str) -> dict[str, str]:
    """
    Normalize WHEEL metadata file for comparison.

    Ignore fields that are expected to differ between build tools:
    - Generator (build tool name)
    - Build (build number, if present)
    """
    ignore_keys = {"Generator", "Build"}
    result = {}
    for line in content.strip().split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key not in ignore_keys:
                result[key] = value
    return result


def normalize_metadata_file(content: str) -> dict[str, list[str]]:
    """
    Normalize METADATA file for comparison.

    Some fields may have different ordering or formatting.
    """
    result: dict[str, list[str]] = {}
    current_key = None
    current_value = []

    for line in content.split("\n"):
        if line.startswith(" ") or line.startswith("\t"):
            # Continuation of previous field
            if current_key:
                current_value.append(line.strip())
        elif ":" in line:
            # Save previous field
            if current_key:
                if current_key in result:
                    result[current_key].append(" ".join(current_value))
                else:
                    result[current_key] = [" ".join(current_value)]

            # Start new field
            key, value = line.split(":", 1)
            current_key = key.strip()
            current_value = [value.strip()]
        elif line == "":
            # End of headers, start of description
            if current_key:
                if current_key in result:
                    result[current_key].append(" ".join(current_value))
                else:
                    result[current_key] = [" ".join(current_value)]
            current_key = "Description-Body"
            current_value = []
        elif current_key == "Description-Body":
            current_value.append(line)

    # Save last field
    if current_key:
        if current_key in result:
            result[current_key].append(" ".join(current_value))
        else:
            result[current_key] = [" ".join(current_value)]

    # Sort multi-value fields for consistent comparison
    for key in result:
        result[key] = sorted(result[key])

    return result


def extract_wheel(wheel_path: Path, extract_dir: Path) -> dict[str, bytes]:
    """Extract wheel and return dict of filename -> content."""
    files = {}
    with zipfile.ZipFile(wheel_path, "r") as zf:
        for name in zf.namelist():
            files[name] = zf.read(name)
    return files


def compare_file_lists(
    files1: dict[str, bytes], files2: dict[str, bytes], name1: str, name2: str
) -> tuple[set[str], set[str], set[str]]:
    """Compare file lists between two wheels."""
    set1 = set(files1.keys())
    set2 = set(files2.keys())

    only_in_1 = set1 - set2
    only_in_2 = set2 - set1
    common = set1 & set2

    return only_in_1, only_in_2, common


def compare_python_files(content1: bytes, content2: bytes, filename: str) -> list[str]:
    """Compare Python source files, ignoring minor whitespace differences."""
    try:
        text1 = content1.decode("utf-8")
        text2 = content2.decode("utf-8")
    except UnicodeDecodeError:
        # Binary comparison
        if content1 == content2:
            return []
        return ["Binary content differs"]

    # Normalize line endings
    lines1 = text1.replace("\r\n", "\n").split("\n")
    lines2 = text2.replace("\r\n", "\n").split("\n")

    if lines1 == lines2:
        return []

    # Generate unified diff
    diff = list(
        difflib.unified_diff(lines1, lines2, fromfile="wheel1", tofile="wheel2", lineterm="")
    )
    return diff


def is_dist_info_file(filename: str) -> bool:
    """Check if file is in the .dist-info directory."""
    return ".dist-info/" in filename


def get_dist_info_type(filename: str) -> str | None:
    """Get the type of dist-info file."""
    if filename.endswith("/RECORD"):
        return "RECORD"
    elif filename.endswith("/WHEEL"):
        return "WHEEL"
    elif filename.endswith("/METADATA"):
        return "METADATA"
    elif filename.endswith("/entry_points.txt"):
        return "entry_points"
    elif filename.endswith("/top_level.txt"):
        return "top_level"
    return None


def compare_wheels(wheel1_path: Path, wheel2_path: Path, verbose: bool = False) -> bool:
    """
    Compare two wheel files.

    Returns True if wheels are equivalent, False otherwise.
    """
    print_header("Comparing Wheels")
    print_info(f"Wheel 1: {wheel1_path}")
    print_info(f"Wheel 2: {wheel2_path}")

    # Validate wheels exist
    if not wheel1_path.exists():
        print_error(f"Wheel 1 not found: {wheel1_path}")
        return False
    if not wheel2_path.exists():
        print_error(f"Wheel 2 not found: {wheel2_path}")
        return False

    # Extract wheels
    print_header("Extracting Wheels")
    files1 = extract_wheel(wheel1_path, Path(tempfile.mkdtemp()))
    files2 = extract_wheel(wheel2_path, Path(tempfile.mkdtemp()))
    print_success(f"Wheel 1 contains {len(files1)} files")
    print_success(f"Wheel 2 contains {len(files2)} files")

    # Compare file lists
    print_header("Comparing File Lists")
    only_in_1, only_in_2, common = compare_file_lists(
        files1, files2, wheel1_path.name, wheel2_path.name
    )

    all_equivalent = True

    if only_in_1:
        print_error("Files only in wheel 1:")
        for f in sorted(only_in_1):
            print(f"    - {f}")
        all_equivalent = False

    if only_in_2:
        print_error("Files only in wheel 2:")
        for f in sorted(only_in_2):
            print(f"    - {f}")
        all_equivalent = False

    if not only_in_1 and not only_in_2:
        print_success(f"Both wheels contain the same {len(common)} files")

    # Compare file contents
    print_header("Comparing File Contents")

    differing_files = []
    identical_files = 0
    metadata_diffs = []

    for filename in sorted(common):
        content1 = files1[filename]
        content2 = files2[filename]

        dist_info_type = get_dist_info_type(filename)

        if dist_info_type == "RECORD":
            # Special handling for RECORD files
            try:
                record1 = normalize_record_file(content1.decode("utf-8"))
                record2 = normalize_record_file(content2.decode("utf-8"))
                if record1 != record2:
                    only_in_record1 = record1 - record2
                    only_in_record2 = record2 - record1
                    metadata_diffs.append(("RECORD", only_in_record1, only_in_record2))
                else:
                    identical_files += 1
            except Exception as e:
                differing_files.append((filename, [f"Error parsing RECORD: {e}"]))

        elif dist_info_type == "WHEEL":
            # Special handling for WHEEL metadata
            try:
                wheel1 = normalize_wheel_file(content1.decode("utf-8"))
                wheel2 = normalize_wheel_file(content2.decode("utf-8"))
                if wheel1 != wheel2:
                    diff_keys = set(wheel1.keys()) ^ set(wheel2.keys())
                    diff_values = {
                        k for k in wheel1.keys() & wheel2.keys() if wheel1[k] != wheel2[k]
                    }
                    if diff_keys or diff_values:
                        metadata_diffs.append(("WHEEL", wheel1, wheel2))
                else:
                    identical_files += 1
                # Always show generator info
                gen1 = "unknown"
                gen2 = "unknown"
                for line in content1.decode("utf-8").split("\n"):
                    if line.startswith("Generator:"):
                        gen1 = line.split(":", 1)[1].strip()
                for line in content2.decode("utf-8").split("\n"):
                    if line.startswith("Generator:"):
                        gen2 = line.split(":", 1)[1].strip()
                print_info(f"Wheel 1 Generator: {gen1}")
                print_info(f"Wheel 2 Generator: {gen2}")
            except Exception as e:
                differing_files.append((filename, [f"Error parsing WHEEL: {e}"]))

        elif dist_info_type == "METADATA":
            # Special handling for METADATA
            try:
                meta1 = normalize_metadata_file(content1.decode("utf-8"))
                meta2 = normalize_metadata_file(content2.decode("utf-8"))
                if meta1 != meta2:
                    metadata_diffs.append(("METADATA", meta1, meta2))
                else:
                    identical_files += 1
            except Exception as e:
                differing_files.append((filename, [f"Error parsing METADATA: {e}"]))

        elif content1 == content2:
            identical_files += 1

        else:
            # Content differs - try to show diff for text files
            diff = compare_python_files(content1, content2, filename)
            if diff:
                differing_files.append((filename, diff))

    print_success(f"{identical_files} files are identical")

    if metadata_diffs:
        print_header("Metadata Differences (Expected)")
        for meta_type, data1, data2 in metadata_diffs:
            if meta_type == "RECORD":
                print_warning("RECORD file differences (file lists):")
                if data1:
                    print(f"    Only in wheel 1: {data1}")
                if data2:
                    print(f"    Only in wheel 2: {data2}")
            elif meta_type == "WHEEL":
                print_warning("WHEEL metadata differences (ignoring Generator):")
                all_keys = set(data1.keys()) | set(data2.keys())
                for key in sorted(all_keys):
                    v1 = data1.get(key, "<missing>")
                    v2 = data2.get(key, "<missing>")
                    if v1 != v2:
                        print(f"    {key}: '{v1}' vs '{v2}'")
                        all_equivalent = False
            elif meta_type == "METADATA":
                print_warning("METADATA differences:")
                all_keys = set(data1.keys()) | set(data2.keys())
                for key in sorted(all_keys):
                    v1 = data1.get(key, ["<missing>"])
                    v2 = data2.get(key, ["<missing>"])
                    if v1 != v2:
                        print(f"    {key}:")
                        print(f"        Wheel 1: {v1}")
                        print(f"        Wheel 2: {v2}")
                        # Only mark as non-equivalent for important fields
                        if key not in {"Description-Body"}:
                            all_equivalent = False

    if differing_files:
        print_header("Content Differences")
        all_equivalent = False
        for filename, diff in differing_files:
            print_error(f"File differs: {filename}")
            if verbose and diff:
                print("    Diff:")
                for line in diff[:50]:  # Limit output
                    print(f"    {line}")
                if len(diff) > 50:
                    print(f"    ... ({len(diff) - 50} more lines)")

    # Summary
    print_header("Summary")
    if all_equivalent:
        print_success("Wheels are functionally equivalent!")
        print_info("(Ignoring expected differences in Generator, timestamps, etc.)")
        return True
    else:
        print_error("Wheels have meaningful differences!")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Compare two wheel files for equivalence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("wheel1", type=Path, help="Path to first wheel file")
    parser.add_argument("wheel2", type=Path, help="Path to second wheel file")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Show detailed diffs for differing files"
    )

    args = parser.parse_args()

    equivalent = compare_wheels(args.wheel1, args.wheel2, args.verbose)
    sys.exit(0 if equivalent else 1)


if __name__ == "__main__":
    main()
