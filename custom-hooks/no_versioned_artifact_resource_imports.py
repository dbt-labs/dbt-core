import os
import sys


def normalize(path: str) -> str:
    """On windows, neither is enough on its own:
    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


def has_bad_artifact_resource_imports(filepath: str) -> bool:
    """
    Checks for improper artifact resource imports outside of the artifacts module.

    Returns:
        True if a file imports from a versioned artifacts module
        False otherwise
    """

    if not filepath.endswith(".py"):
        # skip non-python files
        return False
    elif normalize("core/dbt/artifacts") in filepath:
        # skip files in the artifacts module
        return False

    with open(filepath, "r") as f:
        lines = f.readlines()

    has_bad_imports = False
    for line_number, line in enumerate(lines):
        line_without_whitespace = line.strip()
        if line_without_whitespace.startswith(
            "from dbt.artifacts.resources.v1"
        ) or line_without_whitespace.startswith("import dbt.artifacts.resources.v1"):
            has_bad_imports = True
            print(
                f"{filepath}:{line_number}: Imports from versioned artifacts resource. Instead import from dbt.artifacts.resource directly."
            )

    return has_bad_imports


def main():
    all_passed = True
    for filepath in sys.argv[1:]:
        if has_bad_artifact_resource_imports(filepath):
            all_passed = False
    return all_passed


if __name__ == "__main__":
    sys.exit(main())
