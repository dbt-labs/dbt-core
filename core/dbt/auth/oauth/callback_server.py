from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse


class OAuthCallbackServer(HTTPServer):
    def __init__(self):
        super().__init__(("localhost", 0), _OAuthCallbackHandler)
        self.platform_oauth_state: str = ""
        self.state_oauth_state: str = ""
        self.result: Optional[dict] = None
        self.error: Optional[str] = None


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    server: OAuthCallbackServer

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if "error" in params:
            error = params["error"][0]
            desc = params.get("error_description", [""])[0]
            message = f"{error}: {desc}" if desc else error
            self.server.error = message
            self._send_response(
                500,
                f"<h1>Error</h1><p>{message}</p>",
            )
            return

        callback_state = params.get("state", [None])[0]

        if callback_state == self.server.state_oauth_state:
            code = params.get("code", [None])[0]
            if not code:
                self.server.error = "redirect missing code parameter"
                self._send_response(500, "<h1>Error</h1><p>Missing code</p>")
                return
            self.server.result = {"dbt_state_oauth": code, "state": callback_state}
        elif callback_state == self.server.platform_oauth_state:
            dbt_state_code = params.get("dbt_state_oauth", [None])[0]
            code = params.get("code", [None])[0]
            account_url = params.get("account_url", [None])[0]

            if dbt_state_code:
                self.server.result = {"dbt_state_oauth": dbt_state_code}
            elif code and account_url:
                self.server.result = {"code": code, "account_url": account_url}
            elif not code:
                self.server.error = "redirect missing code parameter"
                self._send_response(500, "<h1>Error</h1><p>Missing code</p>")
                return
            else:
                self.server.error = "redirect missing account_url parameter"
                self._send_response(500, "<h1>Error</h1><p>Missing account_url</p>")
                return
        else:
            self.server.error = "invalid OAuth state parameter"
            self._send_response(500, "<h1>Error</h1><p>Invalid state</p>")
            return

        self._send_response(
            200,
            "<h1>Success</h1><p>You have logged in. You can close this window.</p>",
        )

    def _send_response(self, status: int, body: str):
        html = f"<!doctype html><html><head><meta charset='UTF-8'/><title>dbt - Login</title></head><body style='text-align:center;font-family:sans-serif'>{body}</body></html>"
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format, *args):
        pass
