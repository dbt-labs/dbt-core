from http.server import SimpleHTTPRequestHandler
from unittest.mock import MagicMock, patch

import pytest

from dbt.task.docs.serve import ServeTask


@pytest.fixture
def serve_task():
    # Set up
    task = ServeTask(config=MagicMock(), args=MagicMock())
    task.config.project_target_path = "."
    task.args.port = 8000
    task.args.host = "127.0.0.1"
    return task


def test_serve_bind_to_127(serve_task):
    serve_task.args.browser = False
    with patch("dbt.task.docs.serve.socketserver.TCPServer") as patched_TCPServer:
        patched_TCPServer.return_value = MagicMock()
        serve_task.run()
        patched_TCPServer.assert_called_once_with(("127.0.0.1", 8000), SimpleHTTPRequestHandler)


def test_serve_bind_to_all(serve_task):
    serve_task.args.browser = False
    serve_task.args.host = ""

    with patch("dbt.task.docs.serve.socketserver.TCPServer") as patched_TCPServer:
        patched_TCPServer.return_value = MagicMock()
        serve_task.run()
        patched_TCPServer.assert_called_once_with(("", 8000), SimpleHTTPRequestHandler)
