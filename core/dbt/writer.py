import os.path

import dbt.clients.system


def write_node(node, target_path, subdirectory, payload):
    node_path = node.get('path')

    full_path = os.path.join(
        target_path,
        subdirectory,
        node.get('package_name'),
        node_path)

    dbt.clients.system.write_file(full_path, payload)

    return full_path
