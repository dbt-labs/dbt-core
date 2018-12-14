import logging

# modules are only imported once -- make sure that we don't have > 1
# because subsequent tunnels will block waiting to acquire the port

server = None


def get_or_create_tunnel(host, port, user, remote_host, remote_port, timeout):
    pass
