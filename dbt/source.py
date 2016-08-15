
import os.path
import fnmatch
from dbt.model import Model, Analysis, CompiledModel, TestModel, Schema, Csv

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

    def get_models(self, model_dirs):
        pattern = "[!.#~]*.sql"
        models = [Model(*model) for model in self.find(model_dirs, pattern)]
        return models

    def get_test_models(self, model_dirs):
        pattern = "[!.#~]*.sql"
        models = [TestModel(*model) for model in self.find(model_dirs, pattern)]
        return models

    def get_analyses(self, analysis_dirs):
        pattern = "[!.#~]*.sql"
        models = [Analysis(*analysis) for analysis in self.find(analysis_dirs, pattern)]
        return models

    def get_compiled(self, target_dir, compilation_type):
        "Get compiled SQL files. compilation_type E {build, test}"
        if compilation_type not in ['build', 'test']:
            raise RuntimeError('Invalid compilation_type. Must be on of ["build", "test]. Got {}'.format(compilation_type))
        pattern = "[!.#~]*.sql"
        source_dir = os.path.join(target_dir, compilation_type)
        compiled_models = [CompiledModel(*model) for model in self.find([source_dir], pattern)]
        return compiled_models

    def get_schemas(self, model_dirs):
        "Get schema.yml files"
        pattern = "[!.#~]*.yml"
        schemas = [Schema(*schema) for schema in self.find(model_dirs, pattern)]
        return schemas

    def get_csvs(self, csv_dirs):
        "Get CSV files"
        pattern = "[!.#~]*.csv"
        csvs = [Csv(*csv) for csv in self.find(csv_dirs, pattern)]
        return csvs

