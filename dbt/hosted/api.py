
import requests
import webbrowser
import threading
import os.path
import yaml

DBT_DIR = os.path.join(os.path.expanduser('~'), '.dbt/')
HOST_FILENAME = "host.yml"
DBT_HOST_FILE = os.path.join(DBT_DIR, HOST_FILENAME)

APP_URL = "http://app.getdbt.com/"
# APP_URL = "http://127.0.0.1:8000/"

API_URL = "{}api/v1".format(APP_URL)

try:
    input = raw_input
except NameError:
    pass

class DbtAPI(object):
    def __init__(self):
        self.token = self.get_token()

    def get_token(self):
        if os.path.exists(DBT_HOST_FILE):
            with open(DBT_HOST_FILE, "r") as fh:
                data = yaml.safe_load(fh)
                return data.get('token', None)
        else:
            return None

    def get_account_id(self):
        if os.path.exists(DBT_HOST_FILE):
            with open(DBT_HOST_FILE, "r") as fh:
                data = yaml.safe_load(fh)
                return data.get('account_id', None)
        else:
            return None

    def fail_if_not_configured(self):
        fail = False

        token = self.get_token()
        if not token:
            print("Missing token.")
            fail = True

        account_id = self.get_account_id()
        if not account_id:
            print("Missing account id.")
            fail = True

        if fail:
            print("Please run `dbt hosted configure` to set up credentials.")
            exit(1)

    def configure(self):
        print("See `https://app.getdbt.com/#/configuration` for assistance.")

        if not os.path.exists(DBT_DIR):
            os.mkdir(DBT_DIR)

        with open(DBT_HOST_FILE, "r") as fh:
            data = yaml.safe_load(fh)
            account_id = data.get('account_id', None)

            if account_id is None:
                display_account_id = "[None]"
            else:
                display_account_id = "[{}]".format(account_id)

            token = data.get('token', None)

            if token is None:
                display_token = "[None]"
            else:
                display_token = "[{}...]".format(token[:4])

        input_token = input("Token {}: ".format(display_token))

        if input_token is not None and input_token is not "":
            token = input_token
        elif token is None:
            print("Did not get a token!")
            exit(1)

        input_account_id = input("Account ID {}: ".format(display_account_id))

        if input_account_id is not None and input_account_id is not "":
            account_id = input_account_id
        elif account_id is None:
            print("Did not get an account id!")
            exit(1)

        with open(DBT_HOST_FILE, "w") as fh:
            data = {
                "account_id": account_id,
                "token": token
            }
            yaml.dump(data, fh)

        print("Wrote {} successfully.".format(DBT_HOST_FILE))

    def headers(self):
        return {"Authorization": "Token {}".format(self.token)}

    def get_or_create_project(self, repo):
        data = {
            "github_repo": repo,
        }

        account_id = self.get_account_id()

        r = requests.post(
            '{}/accounts/{}/projects/'.format(API_URL, account_id),
            headers=self.headers(),
            json=data)


        if r.status_code == 201:
            print("Pushed project '{}' successfully.".format(repo))
        elif r.status_code == 401:
            print("You don't have access to add a project for the specified account. Please run `dbt hosted configure` and verify the credentials you've provided.")
        else:
            print("ERROR: Something went wrong pushing project '{}'".format(repo))

    def create_or_update_active_profiles(self, project):
        if not self.get_token():
            print("No token found, please use `dbt hosted configure` to set one up.")
            return

        account_id = self.get_account_id()

        if not account_id:
            print("No account id found, please use `dbt hosted configure` to set one up.")
            return

        for profile_name in project.active_profile_names:
            profile = project.profiles[profile_name]

            to_send = {
                'name': profile_name,
                'yaml': yaml.dump(profile)
            }

            r = requests.post('{}/accounts/{}/profiles/'.format(API_URL, account_id),
                              headers=self.headers(),
                              data=to_send)

            if r.status_code == 201:
                print("Pushed profile '{}' successfully.".format(profile_name))
            elif r.status_code == 401:
                print("You don't have access to add a profile for the specified account. Please run `dbt hosted configure` and verify the credentials you've provided.")
            else:
                print("ERROR: Something went wrong pushing profile '{}'".format(profile_name))

    def authenticate(self):
        if not webbrowser.open_new(APP_URL):
            raise RuntimeError("Couldn't open web browser")

        return None
