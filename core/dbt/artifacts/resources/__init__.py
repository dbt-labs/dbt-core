from dbt.artifacts.resources.base import BaseResource

# alias to latest resource definitions
from dbt.artifacts.resources.v1.documentation import Documentation
from dbt.artifacts.resources.v1.macro import Macro, MacroDependsOn, MacroArgument
from dbt.artifacts.resources.v1.docs import Docs
from dbt.artifacts.resources.v1.group import Group
from dbt.artifacts.resources.v1.owner import Owner
