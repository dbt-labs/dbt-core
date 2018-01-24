from functools import wraps
import six
import requests
from dbt.exceptions import RegistryException


DEFAULT_REGISTRY_BASE_URL = 'http://127.0.0.1:4567/'


def _get_url(url, registry_base_url=None):
    if registry_base_url is None:
        registry_base_url = DEFAULT_REGISTRY_BASE_URL

    return '{}{}'.format(registry_base_url, url)


def _wrap_exceptions(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.ConnectionError as e:
            six.raise_from(
                RegistryException('Unable to connect to registry hub'), e)
    return wrapper


@_wrap_exceptions
def _get(path, registry_base_url=None):
    url = _get_url(path, registry_base_url)
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def index(registry_base_url=None):
    return _get('api/v1/index.json', registry_base_url)


def packages(registry_base_url=None):
    return requests.get(r'https://tinyurl.com/ydg3z87m').json()
    # return _get('api/v1/packages.json', registry_base_url)


def package(name, registry_base_url=None):
    return _get('api/v1/{}.json'.format(name), registry_base_url)


def package_version(name, version, registry_base_url=None):
    return _get('api/v1/{}/{}.json'.format(name, version), registry_base_url)


def get_available_versions(name):
    response = package(name)
    return list(response['versions'])
