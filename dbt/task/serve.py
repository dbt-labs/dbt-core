import shutil
import os
import webbrowser
from socketserver import TCPServer

from dbt.include import DOCS_INDEX_FILE_PATH
from dbt.compat import SimpleHTTPRequestHandler
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.base_task import RunnableTask


class ServeTask(RunnableTask):
    def run(self):
        os.chdir(self.config.target_path)

        port = self.args.port

        shutil.copyfile(DOCS_INDEX_FILE_PATH, 'index.html')

        logger.info("Serving docs at 0.0.0.0:{}".format(port))
        logger.info(
            "To access from your browser, navigate to http://localhost:{}."
            .format(port)
        )
        logger.info("Press Ctrl+C to exit.\n\n")

        httpd = TCPServer(
            ('0.0.0.0', port),
            SimpleHTTPRequestHandler
        )

        try:
            webbrowser.open_new_tab('http://127.0.0.1:{}'.format(port))
        except webbrowser.Error as e:
            pass

        try:
            httpd.serve_forever()  # blocks
        finally:
            httpd.shutdown()
            httpd.server_close()

        return None
