
from dbt.runner import RunManager
from dbt.templates import ArchiveInsertTemplate
from dbt.compilation import Compiler

class ArchiveTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project
        self.create_template = ArchiveInsertTemplate

    def compile(self):
        compiler = Compiler(self.project, self.create_template)
        compiler.initialize()
        compiled = compiler.compile_archives()
        print("Compiled {} archives".format(len(compiled)))

    def run(self):
        self.compile()
        runner = RunManager(self.project, self.project['target-path'], self.create_template.label, self.args.threads)

        results = runner.run_archive()

