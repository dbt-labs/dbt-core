from dbt.compilation import Compiler, CompilableEntities
from dbt.templates import BaseCreateTemplate, DryCreateTemplate


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        if self.args.dry:
            create_template = DryCreateTemplate
        else:
            create_template = BaseCreateTemplate

        compiler = Compiler(self.project, create_template, self.args)
        compiler.initialize()
        results = compiler.compile(limit_to=CompilableEntities)

        stat_line = ", ".join(
            ["{} {}".format(results[k], k) for k in CompilableEntities]
        )
        print("Compiled {}".format(stat_line))
