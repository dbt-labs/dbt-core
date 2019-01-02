

def named_property(name, doc=None):
    def get_prop(self):
        return self._contents.get(name)

    def set_prop(self, value):
        self._contents[name] = value
        self.validate()

    return property(get_prop, set_prop, doc=doc)
