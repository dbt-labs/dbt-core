from string import ascii_lowercase
import re

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

REMOTE_VERSION_FILE = \
    'https://raw.githubusercontent.com/fishtown-analytics/dbt/' \
    'master/.bumpversion.cfg'


class Version(object):

    def __init__(self, name, is_latest=True):
        self.name = name
        self.parts = self.get_parts()
        self.numeric_parts = self.get_numeric_parts()
        self.is_latest = is_latest

    def get_parts(self):
        return self.name.split('.')

    def get_numeric_parts(self):
        numeric_name = ''.join(
            [ch for ch in self.name if ch not in ascii_lowercase])
        return numeric_name.split('.')

    def __str__(self):
        if self.is_latest:
            return "Current version: {}\n".format(self.name)
        return "Installed version: {}\n".format(self.name)

    def __eq__(self, other):
        if not isinstance(other, Version):
            return NotImplemented
        return self.name == other.name

    def __gt__(self, other):
        if not isinstance(other, Version):
            return NotImplemented

        if len(self.parts) > len(other.parts):
            return True

        zipped_parts = zip(self.numeric_parts, other.numeric_parts)
        for self_part, other_part in zipped_parts:
            if other_part > self_part:
                return False
            if self_part > other_part:
                return True
        return False

    def __lt__(self, other):
        if not isinstance(other, Version):
            return NotImplemented

        if len(self.parts) < len(other.parts):
            return True

        zipped_parts = zip(self.numeric_parts, other.numeric_parts)
        for self_part, other_part in zipped_parts:
            if other_part > self_part:
                return True
            if self_part > other_part:
                return False
        return False


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
    except:
        contents = ''
    if hasattr(contents, 'decode'):
        contents = contents.decode('utf-8')
    return contents


def get_latest_version():
    contents = get_remote_version_file_contents()
    version_string = get_version_string_from_text(contents)
    return Version(version_string)


def get_installed_version():
    return Version(__version__, is_latest=False)


def get_version_information():
    installed = get_installed_version()
    latest = get_latest_version()
    if installed == latest:
        return "{}{}Up to date!".format(installed, latest)

    elif installed > latest:
        return "{}{}Your version is ahead!".format(
                installed, latest)
    else:
        return "{}{}Your version of dbt is out of date!\n" \
            "\tYou can find instructions for upgrading here:\n" \
            "\thttps://docs.getdbt.com/docs/installation" \
            .format(installed, latest)


__version__ = '0.9.0'
installed = get_installed_version()
latest = get_latest_version()
