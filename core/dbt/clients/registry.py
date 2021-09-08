import functools
import requests
from dbt.utils import memoized, _connection_exception_retry as connection_exception_retry
from dbt.logger import GLOBAL_LOGGER as logger
from dbt import deprecations
import os

if os.getenv('DBT_PACKAGE_HUB_URL'):
    DEFAULT_REGISTRY_BASE_URL = os.getenv('DBT_PACKAGE_HUB_URL')
else:
    DEFAULT_REGISTRY_BASE_URL = 'https://hub.getdbt.com/'


def _get_url(url, registry_base_url=None):
    if registry_base_url is None:
        registry_base_url = DEFAULT_REGISTRY_BASE_URL

    return '{}{}'.format(registry_base_url, url)


def _get_with_retries(path, registry_base_url=None):
    get_fn = functools.partial(_get, path, registry_base_url)
    return connection_exception_retry(get_fn, 5)


def _get(path, registry_base_url=None):
    url = _get_url(path, registry_base_url)
    logger.debug('Making package registry request: GET {}'.format(url))
    resp = requests.get(url, timeout=30)
    logger.debug('Response from registry: GET {} {}'.format(url,
                                                            resp.status_code))
    resp.raise_for_status()
    return resp.json()


def index(registry_base_url=None):
    return _get_with_retries('api/v1/index.json', registry_base_url)


index_cached = memoized(index)


def packages(registry_base_url=None):
    return _get_with_retries('api/v1/packages.json', registry_base_url)


def package(name, registry_base_url=None):
    response = _get_with_retries('api/v1/{}.json'.format(name), registry_base_url)

    # Either redirectnamespace or redirectname in the JSON response indicate a redirect
    # redirectnamespace redirects based on package ownership
    # redirectname redirects based on package name
    # Both can be present at the same time, or neither. Fails gracefully to old name

    if ('redirectnamespace' in response) or ('redirectname' in response):

        if ('redirectnamespace' in response) and response['redirectnamespace'] is not None:
            use_namespace = response['redirectnamespace']
        else:
            use_namespace = response['namespace']

        if ('redirectname' in response) and response['redirectname'] is not None:
            use_name = response['redirectname']
        else:
            use_name = response['name']

        new_nwo = use_namespace + "/" + use_name
        deprecations.warn('package-redirect', old_name=name, new_name=new_nwo)

    return response


def package_version(name, version, registry_base_url=None):
    return _get_with_retries('api/v1/{}/{}.json'.format(name, version), registry_base_url)


def get_available_versions(name):
    response = package(name)
    return list(response['versions'])
