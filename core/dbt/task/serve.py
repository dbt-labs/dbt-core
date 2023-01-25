import os
import shutil
import socketserver
import webbrowser
from http.server import SimpleHTTPRequestHandler

import click

from dbt.include.global_project import DOCS_INDEX_FILE_PATH
from dbt.task.base import ConfiguredTask


class ServeTask(ConfiguredTask):
    def run(self):
        os.chdir(self.config.target_path)
        shutil.copyfile(DOCS_INDEX_FILE_PATH, "index.html")

        port = self.args.port

        if self.args.browser:
            webbrowser.open_new_tab(f"http://localhost:{port}")

        with socketserver.TCPServer(("", port), SimpleHTTPRequestHandler) as httpd:
            click.echo(f"Serving docs at {port}")
            click.echo(f"To access from your browser, navigate to: http://localhost:{port}")
            click.echo("\n\n")
            click.echo("Press Ctrl+C to exit.")
            httpd.serve_forever()
