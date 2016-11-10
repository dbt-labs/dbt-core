
import dbt.project
from dbt.compilation import Compiler
from dbt.templates import BaseCreateTemplate, DryCreateTemplate

def new_project():
    return dbt.project.read_project('tests/test-project/dbt_project.yml')

def new_base_compiler():
    project = new_project()
    create_template = BaseCreateTemplate
    compiler = Compiler(project, create_template)
    return compiler

def new_test_compiler():
    project = new_project()
    create_template = DryCreateTemplate
    compiler = Compiler(project, create_template)
    return compiler
