{% macro uses_bad_stuff() %}

   {{ref('dep_macro')}}
--   {{this}}
--   {{var('test')}}

{% endmacro %}

{% macro invalid_macro() %}

    select
        '{{ foo2 }}' as foo2,
        '{{ bar2 }}' as bar2
