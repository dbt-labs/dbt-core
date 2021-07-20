import argparse
import json
import secrets
import textwrap
from base64 import b64encode

import requests
from werkzeug.utils import redirect
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from urllib.parse import urlencode


def _make_rfp_claim_value():
    # from https://tools.ietf.org/id/draft-bradley-oauth-jwt-encoded-state-08.html#rfc.section.4  # noqa
    # we can do whatever we want really, so just token.urlsafe?
    return secrets.token_urlsafe(112)


def _make_response(client_id, client_secret, refresh_token):
    return Response(textwrap.dedent(
        f'''\
        SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN="{refresh_token}"
        SNOWFLAKE_TEST_OAUTH_CLIENT_ID="{client_id}"
        SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET="{client_secret}"'''
    ))


class TokenManager:
    def __init__(self, account_name, client_id, client_secret):
        self.account_name = account_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.rfp_claim = _make_rfp_claim_value()
        self.port = 8080

    @property
    def account_url(self):
        return f'https://{self.account_name}.snowflakecomputing.com'

    @property
    def auth_url(self):
        return f'{self.account_url}/oauth/authorize'

    @property
    def token_url(self):
        return f'{self.account_url}/oauth/token-request'

    @property
    def redirect_uri(self):
        return f'http://localhost:{self.port}'

    @property
    def headers(self):
        auth = f'{self.client_id}:{self.client_secret}'.encode('ascii')
        encoded_auth = b64encode(auth).decode('ascii')
        return {
            'Authorization': f'Basic {encoded_auth}',
            'Content-type': 'application/x-www-form-urlencoded; charset=utf-8'
        }

    def _code_to_token(self, code):
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
        }
        # data = urlencode(data)
        resp = requests.post(
            url=self.token_url,
            headers=self.headers,
            data=data,
        )
        try:
            refresh_token = resp.json()['refresh_token']
        except KeyError:
            print(resp.json())
            raise
        return refresh_token

    @Request.application
    def auth(self, request):
        code = request.args.get('code')
        if code:
            # we got 303'ed here with a code
            state_received = request.args.get('state')
            if state_received != self.rfp_claim:
                return Response('Invalid RFP claim: MITM?', status=401)
            refresh_token = self._code_to_token(code)
            return _make_response(
                self.client_id,
                self.client_secret,
                refresh_token,
            )
        else:
            return redirect('/login')

    @Request.application
    def login(self, request):
        # take the auth URL and add the query string to it
        query = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'state': self.rfp_claim,
        }
        query = urlencode(query)
        return redirect(f'{self.auth_url}?{query}')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('account_name', help='The account name')
    parser.add_argument('json_blob', help='The json auth blob')

    return parser.parse_args()


def main():
    args = parse_args()
    data = json.loads(args.json_blob)
    client_id = data['OAUTH_CLIENT_ID']
    client_secret = data['OAUTH_CLIENT_SECRET']
    token_manager = TokenManager(
        account_name=args.account_name,
        client_id=client_id,
        client_secret=client_secret,
    )
    app = DispatcherMiddleware(token_manager.auth, {
        '/login': token_manager.login,
    })

    run_simple('localhost', token_manager.port, app)


if __name__ == '__main__':
    main()
