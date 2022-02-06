-- Macro to override ref and always return the same result
{% macro ref(modelname) %}
{% do return(builtins.ref(modelname).replace_path(identifier='seed_2')) %}
{% endmacro %}