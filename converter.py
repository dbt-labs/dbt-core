#!/usr/bin/env python
import json
import yaml
import sys
import argparse
from datetime import datetime, timezone
import dbt.clients.registry as registry


def yaml_type(fname):
    with open(fname) as f:
        return yaml.load(f)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=yaml_type, default="dbt_project.yml")
    parser.add_argument("--namespace", required=True)
    return parser.parse_args()


def get_full_name(args):
    return "{}/{}".format(args.namespace, args.project["name"])


def init_project_in_packages(args, packages):
    full_name = get_full_name(args)
    if full_name not in packages:
        packages[full_name] = {
            "name": args.project["name"],
            "namespace": args.namespace,
            "latest": args.project["version"],
            "assets": {},
            "versions": {},
        }
    return packages[full_name]


def add_version_to_package(args, project_json):
    project_json["versions"][args.project["version"]] = {
        "id": "{}/{}".format(get_full_name(args), args.project["version"]),
        "name": args.project["name"],
        "version": args.project["version"],
        "description": "",
        "published_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "packages": args.project.get("packages") or [],
        "works_with": [],
        "_source": {
            "type": "github",
            "url": "",
            "readme": "",
        },
        "downloads": {
            "tarball": "",
            "format": "tgz",
            "sha1": "",
        },
    }


def main():
    args = parse_args()
    packages = registry.packages()
    project_json = init_project_in_packages(args, packages)
    if args.project["version"] in project_json["versions"]:
        raise Exception("Version {} already in packages JSON"
                        .format(args.project["version"]),
                        file=sys.stderr)
    add_version_to_package(args, project_json)
    print(json.dumps(packages, indent=2))

if __name__ == "__main__":
    main()
