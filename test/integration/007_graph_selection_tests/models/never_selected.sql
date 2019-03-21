{{
	config(schema='_and_then')
}}

select * from {{ this.schema }}.seed
