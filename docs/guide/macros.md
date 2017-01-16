# Macros

Macros are snippets of SQL that can be invoked like functions from models. Macros make it possible to re-use SQL between models
in keeping with the engineering principle of DRY (Don't Repeat Yourself). Additionally, shared packages can expose macros
that you can use in your own dbt project.

To use macros, add a `macro-paths` config entry to your `dbt_project.yml` file. Macro files must use the `.sql` file extension.

```yml
# dbt_project.yml
source-paths: ["models"]
target-path: "target"
...
macro-paths: ['macros']    # look for macros in ./macros directory
```

Macro files can contain one or more macros. An example macro file looks like:

```sql
-- ./macros/my_macros.sql

-- generates a GROUP BY 1,2,3,4,5... statement
{% macro group_by(n) %}

  GROUP BY
   {% for i in range(1, n + 1) %}
     {{ i }}
     {% if not loop.last %} , {% endif %}
   {% endfor %}

{% endmacro %}

```

Here, we define a macro called `group_by` which takes a single argument, n. A model which uses this macro might look like:

```sql
-- models/my_model.sql

select
  field_1,
  field_2,
  field_3,
  field_4,
  field_5,
  count(*)
from my_table
{{ group_by(5) }}
```

which would be _compiled_ to:

```sql
select
  field_1,
  field_2,
  field_3,
  field_4,
  field_5,
  count(*)
from my_table
GROUP BY 1,2,3,4,5
```

In the above example, we could reference the macro directly because it lives in our own project.
If the macro was imported from the `dbt_sql_helpers` package via:

```yml
# dbt_project.yml
...
repositories:
    # this package's "name" is dbt_sql_helpers in its dbt_project.yml file!
  - "https://github.com/fishtown-analytics/dbt_sql_helpers.git"
```

then we would need to fully-qualify the macro:


```sql
-- models/my_model.sql
select
  field_1,
  field_2,
  field_3,
  field_4,
  field_5,
  count(*)
from my_table
{{ dbt_sql_helpers.group_by(5) }}
```
