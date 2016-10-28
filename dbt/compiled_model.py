import hashlib
import jinja2
from dbt.utils import compiler_error

class CompiledModel(object):
    def __init__(self, fqn, data):
        self.fqn = fqn
        self.data = data
        self.nice_name = ".".join(fqn)

        # these are set just before the models are executed
        self.tmp_drop_type = None
        self.final_drop_type = None
        self.target = None

        self.skip = False
        self._contents = None
        self.compiled_contents = None

    def __getitem__(self, key):
        return self.data[key]

    def hashed_name(self):
        fqn_string = ".".join(self.fqn)
        return hashlib.md5(fqn_string.encode('utf-8')).hexdigest()

    def context(self):
        return self.data

    def hashed_contents(self):
        return hashlib.md5(self.contents.encode('utf-8')).hexdigest()

    def do_skip(self):
        self.skip = True

    def should_skip(self):
        return self.skip

    def is_type(self, run_type):
        return self.data['dbt_run_type'] == run_type

    @property
    def contents(self):
        if self._contents is None:
            with open(self.data['build_path']) as fh:
                self._contents = fh.read()
        return self._contents

    def compile(self, context):
        contents = self.contents
        try:
            env = jinja2.Environment()
            self.compiled_contents = env.from_string(contents).render(context)
            return self.compiled_contents
        except jinja2.exceptions.TemplateSyntaxError as e:
            compiler_error(self, str(e))

    @property
    def materialization(self):
        return self.data['materialized']

    @property
    def name(self):
        return self.data['name']

    @property
    def tmp_name(self):
        return self.data['tmp_name']

    def project(self):
        return {'name': self.data['project_name']}

    @property
    def schema(self):
        if self.target is None:
            raise RuntimeError("`target` not set in compiled model {}".format(self))
        else:
            return self.target.schema

    def should_execute(self):
        return self.data['enabled'] and self.materialization != 'ephemeral'

    def should_rename(self):
        return self.data['materialized'] in ['table' , 'view']

    def prepare(self, existing, target):
        if self.materialization == 'incremental':
            tmp_drop_type = None
            final_drop_type = None
        else:
            tmp_drop_type = existing.get(self.tmp_name, None) 
            final_drop_type = existing.get(self.name, None)

        self.tmp_drop_type = tmp_drop_type
        self.final_drop_type = final_drop_type
        self.target = target

    def __repr__(self):
        return "<CompiledModel {}.{}: {}>".format(self.data['project_name'], self.name, self.data['build_path'])

class CompiledTest(CompiledModel):
    def __init__(self, fqn, data):
        super(CompiledTest, self).__init__(fqn, data)

    def should_rename(self):
        return False

    def should_execute(self):
        return True

    def prepare(self, existing, target):
        self.target = target

    def __repr__(self):
        return "<CompiledModel {}.{}: {}>".format(self.data['project_name'], self.name, self.data['build_path'])

class CompiledArchive(CompiledModel):
    def __init__(self, fqn, data):
        super(CompiledArchive, self).__init__(fqn, data)

    def should_rename(self):
        return False

    def should_execute(self):
        return True

    def prepare(self, existing, target):
        self.target = target

    def __repr__(self):
        return "<CompiledArchive {}.{}: {}>".format(self.data['project_name'], self.name, self.data['build_path'])

def make_compiled_model(fqn, data):
    run_type = data['dbt_run_type']

    if run_type in ['run', 'dry-run']:
        return CompiledModel(fqn, data)
    elif run_type == 'test':
        return CompiledTest(fqn, data)
    elif run_type == 'archive':
        return CompiledArchive(fqn, data)
    else:
        raise RuntimeError("invalid run_type given: {}".format(run_type))


