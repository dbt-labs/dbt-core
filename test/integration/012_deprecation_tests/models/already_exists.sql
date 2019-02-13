select 1 as id

{% if adapter.already_exists(this.schema, this.identifier) and not flags.FULL_REFRESH %}
	where id > (select max(id) from {{this}})
{% endif %}
