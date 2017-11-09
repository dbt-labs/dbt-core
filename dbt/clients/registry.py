import requests


DEFAULT_REGISTRY_BASE_URL = 'http://127.0.0.1:35729/'


def _get_url(url, registry_base_url=None):
    if registry_base_url is None:
        registry_base_url = DEFAULT_REGISTRY_BASE_URL

    return '{}{}'.format(registry_base_url, url)


def index(registry_base_url=None):
    return requests.get(
        _get_url('api/v1/index.json',
                 registry_base_url)).json()


def package(name, registry_base_url=None):
    return requests.get(
        _get_url('api/v1/{}.json'.format(name),
                 registry_base_url)).json()


def package_version(name, version, registry_base_url=None):
    url = _get_url('api/v1/{}/{}.json'.format(name, version))
    response = requests.get(url, registry_base_url)

    return response.json()


def get_available_versions(name):
    response = package(name)

    return list(response['versions'].keys())
