from dbt.utils import compiler_error


class RuntimeContext(dict):
    def __init__(self, model=None, *args, **kwargs):
        super(RuntimeContext, self).__init__(*args, **kwargs)

        self.model = model

    def __getattr__(self, attr):
        if attr in self:
            return self.get(attr)
        else:
            compiler_error(self.model, "'{}' is undefined".format(attr))

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(RuntimeContext, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(RuntimeContext, self).__delitem__(key)
        del self.__dict__[key]

    def update_global(self, data):
        self.update(data)

    def update_package(self, pkg_name, data):
        if pkg_name not in self:
            ctx = RuntimeContext(model=self.model)

            self[pkg_name] = ctx

        self[pkg_name].update(data)
