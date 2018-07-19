import json
import os

from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestLateBindingView(DBTIntegrationTest):
    @property
    def schema(self):
        return 'late_binding_view_033'

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/033_redshift_test', path)
        )

    @property
    def models(self):
        return self.dir("models")
