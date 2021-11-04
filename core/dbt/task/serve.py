import shutil
import os
import webbrowser

from dbt.include.global_project import DOCS_INDEX_FILE_PATH
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer
from dbt.events.functions import fire_event
from dbt.events.types import ServingDocsPort, ServingDocsAccessInfo, ServingDocsExitInfo, EmptyLine

from dbt.task.base import ConfiguredTask


class ServeTask(ConfiguredTask):
    def run(self):
        os.chdir(self.config.target_path)

        port = self.args.port
        address = "0.0.0.0"

        shutil.copyfile(DOCS_INDEX_FILE_PATH, "index.html")

        fire_event(ServingDocsPort(address=address, port=port))
        fire_event(ServingDocsAccessInfo(port=port))
        fire_event(EmptyLine())
        fire_event(EmptyLine())
        fire_event(ServingDocsExitInfo())

        # mypy doesn't think SimpleHTTPRequestHandler is ok here, but it is
        httpd = TCPServer(  # type: ignore
            (address, port), SimpleHTTPRequestHandler  # type: ignore
        )  # type: ignore

        if self.args.open_browser:
            try:
                webbrowser.open_new_tab(f"http://127.0.0.1:{port}")
            except webbrowser.Error:
                pass

        try:
            httpd.serve_forever()  # blocks
        finally:
            httpd.shutdown()
            httpd.server_close()

        return None
