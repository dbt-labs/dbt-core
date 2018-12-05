import re

import dbt.semver

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

REMOTE_VERSION_FILE = \
    'https://raw.githubusercontent.com/fishtown-analytics/dbt/' \
    'master/.bumpversion.cfg'


def get_version_string_from_text(contents):
    matches = re.search(r"current_version = ([\.0-9a-z]+)", contents)
    if matches is None or len(matches.groups()) != 1:
        return ""
    version = matches.groups()[0]
    return version


def get_remote_version_file_contents(url=REMOTE_VERSION_FILE):
    try:
        f = urlopen(url)
        contents = f.read()
    except Exception:
        contents = ''
    if hasattr(contents, 'decode'):
        contents = contents.decode('utf-8')
    return contents


def get_latest_version():
    contents = get_remote_version_file_contents()
    if contents == '':
        return None
    version_string = get_version_string_from_text(contents)
    return dbt.semver.VersionSpecifier.from_version_string(version_string)


def get_installed_version():
    return dbt.semver.VersionSpecifier.from_version_string(__version__)


def get_version_information():
    installed = get_installed_version()
    latest = get_latest_version()

    installed_s = installed.to_version_string(skip_matcher=True)
    if latest is None:
        latest_s = 'unknown'
    else:
        latest_s = latest.to_version_string(skip_matcher=True)

    version_msg = ("installed version: {}\n"
                   "   latest version: {}\n\n".format(installed_s, latest_s))

    if latest is None:
        return ("{}The latest version of dbt could not be determined!\n"
                "Make sure that the following URL is accessible:\n{}"
                .format(version_msg, REMOTE_VERSION_FILE))

    if installed == latest:
        return "{}Up to date!".format(version_msg)

    elif installed > latest:
        return ("{}Your version of dbt is ahead of the latest "
                "release!".format(version_msg))

    else:
        return ("{}Your version of dbt is out of date! "
                "You can find instructions for upgrading here:\n"
                "https://docs.getdbt.com/docs/installation"
                .format(version_msg))


__version__ = '0.12.2a1'
installed = get_installed_version()
