{#
	We are running against a database that must be quoted.
	These calls ensure that we trigger an error if we're failing to quote at parse-time
#}
{% do adapter.already_exists(this.schema, this.table) %}
{% do adapter.get_relation(this.database, this.schema, this.table) %}
select * from {{ ref('seed') }}
