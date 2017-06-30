from dbt.model import Csv

import dbt.clients.system


class Source(object):
    def __init__(self, project, own_project=None):
        self.project = project
        self.project_root = project['project-root']
        self.project_name = project['name']

        self.own_project = (own_project if own_project is not None
                            else self.project)
        self.own_project_root = self.own_project['project-root']
        self.own_project_name = self.own_project['name']

    def build_models_from_file_matches(
            self,
            to_build,
            file_matches,
            extra_args=[]):

        build_args = [[self.project,
                       file_match.get('searched_path'),
                       file_match.get('relative_path'),
                       self.own_project] + extra_args
                      for file_match in file_matches]

        return [to_build(*args) for args in build_args]

    def get_csvs(self, csv_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            csv_dirs,
            "[!.#~]*.csv")

        return self.build_models_from_file_matches(
            Csv,
            file_matches)
