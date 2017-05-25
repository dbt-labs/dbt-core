import errno
import fnmatch
import os
import os.path
import sys


def find_matching(root_path,
                  relative_paths_to_search,
                  file_pattern):
    """
    Given an absolute `root_path`, a list of relative paths to that
    absolute root path (`relative_paths_to_search`), and a `file_pattern`
    like '*.sql', returns information about the files. For example:

    > find_matching('/root/path', 'models', '*.sql')

      [ { 'absolute_path': '/root/path/models/model_one.sql',
          'relative_path': 'models/model_one.sql',
          'searched_path': 'models' },
        { 'absolute_path': '/root/path/models/subdirectory/model_two.sql',
          'relative_path': 'models/subdirectory/model_two.sql',
          'searched_path': 'models' } ]
    """
    matching = []

    for relative_path_to_search in relative_paths_to_search:
        absolute_path_to_search = os.path.join(
            root_path, relative_path_to_search)
        walk_results = os.walk(absolute_path_to_search)

        for current_path, subdirectories, local_files in walk_results:
            for local_file in local_files:
                absolute_path = os.path.join(current_path, local_file)
                relative_path = os.path.relpath(
                    absolute_path, absolute_path_to_search)

                if fnmatch.fnmatch(local_file, file_pattern):
                    matching.append({
                        'searched_path': relative_path_to_search,
                        'absolute_path': absolute_path,
                        'relative_path': relative_path,
                    })

    return matching


def load_file_contents(path, strip=True):
    with open(path, 'rb') as handle:
        to_return = handle.read().decode('utf-8')

    if strip:
        to_return = to_return.strip()

    return to_return


def make_directory(path):
    """
    Make a directory and any intermediate directories that don't already
    exist. This function handles the case where two threads try to create
    a directory at once.
    """
    if not os.path.exists(path):
        # concurrent writes that try to create the same dir can fail
        try:
            os.makedirs(path)

        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise e


def make_file(path, contents='', overwrite=False):
    """
    Make a file at `path` assuming that the directory it resides in already
    exists. The file is saved with contents `contents`
    """
    if overwrite or not os.path.exists(path):
        with open(path, 'w') as fh:
            fh.write(contents)
        return True

    return False


def open_dir_cmd():
    # https://docs.python.org/2/library/sys.html#sys.platform
    if sys.platform == 'win32':
        return 'start'

    elif sys.platform == 'darwin':
        return 'open'

    else:
        return 'xdg-open'
