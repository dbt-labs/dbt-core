import os
import pytest

pytestmark = pytest.mark.skipif(os.name == 'nt', reason='"dbt rpc" not supported on windows')
