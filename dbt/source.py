
import os.path
import fnmatch
from dbt.model import Model, Analysis, TestModel, SchemaFile, Csv, Macro, ArchiveModel

class Source(object):
    def __init__(self, project, own_project=None):
        self.project = project
        self.project_root = project['project-root']
        self.project_name = project['name']

        self.own_project = own_project if own_project is not None else self.project
        self.own_project_root = self.own_project['project-root']
        self.own_project_name = self.own_project['name']

    def find(self, source_paths, file_pattern):
        """returns abspath, relpath, filename of files matching file_regex in source_paths"""
        found = []
        for source_path in source_paths:
            root_path = os.path.join(self.own_project_root, source_path)
            for root, dirs, files in os.walk(root_path):
                for filename in files:
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, root_path)

                    if fnmatch.fnmatch(filename, file_pattern):
                        found.append((self.project, source_path, rel_path, self.own_project))
        return found

    def get_models(self, model_dirs, create_template):
        pattern = "[!.#~]*.sql"
        models = [Model(*model + (create_template,)) for model in self.find(model_dirs, pattern)]
        return models

    def get_test_models(self, model_dirs, create_template):
        pattern = "[!.#~]*.sql"
        models = [TestModel(*model + (create_template,)) for model in self.find(model_dirs, pattern)]
        return models

    def get_analyses(self, analysis_dirs):
        pattern = "[!.#~]*.sql"
        models = [Analysis(*analysis) for analysis in self.find(analysis_dirs, pattern)]
        return models

    def get_schemas(self, model_dirs):
        "Get schema.yml files"
        pattern = "[!.#~]*.yml"
        schemas = [SchemaFile(*schema) for schema in self.find(model_dirs, pattern)]
        return schemas

    def get_csvs(self, csv_dirs):
        "Get CSV files"
        pattern = "[!.#~]*.csv"
        csvs = [Csv(*csv) for csv in self.find(csv_dirs, pattern)]
        return csvs

    def get_macros(self, macro_dirs):
        "Get Macro files"
        pattern = "[!.#~]*.sql"
        macros = [Macro(*macro) for macro in self.find(macro_dirs, pattern)]
        return macros

    def get_archives(self, create_template):
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
                archives.append(ArchiveModel(self.project, create_template, fields))
        return archives


