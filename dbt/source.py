import os.path
import fnmatch
from dbt.model import Model, Analysis, SchemaFile, Csv, Macro, \
    ArchiveModel, DataTest

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

    def get_models(self, model_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            model_dirs,
            "[!.#~]*.sql")

        return self.build_models_from_file_matches(
            Model,
            file_matches)

    def get_analyses(self, analysis_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            analysis_dirs,
            "[!.#~]*.sql")

        return self.build_models_from_file_matches(
            Analysis,
            file_matches)

    def get_schemas(self, schema_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            schema_dirs,
            "[!.#~]*.yml")

        return self.build_models_from_file_matches(
            SchemaFile,
            file_matches)

    def get_tests(self, test_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            test_dirs,
            "[!.#~]*.sql")

        return self.build_models_from_file_matches(
            DataTest,
            file_matches)

    def get_csvs(self, csv_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            csv_dirs,
            "[!.#~]*.csv")

        return self.build_models_from_file_matches(
            Csv,
            file_matches)

    def get_macros(self, macro_dirs):
        file_matches = dbt.clients.system.find_matching(
            self.own_project_root,
            macro_dirs,
            "[!.#~]*.sql")

        return self.build_models_from_file_matches(
            Macro,
            file_matches)

    def get_archives(self):
        "Get Archive models defined in project config"

        if 'archive' not in self.project:
            return []

        raw_source_schemas = self.project['archive']

        archives = []
        for schema in raw_source_schemas:
            schema = schema.copy()
            if 'tables' not in schema:
                continue

            tables = schema.pop('tables')
            for table in tables:
                fields = table.copy()
                fields.update(schema)
                archives.append(ArchiveModel(
                    self.project, fields
                ))
        return archives
