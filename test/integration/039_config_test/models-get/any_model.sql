-- models/any_model.sql
select {{ config.get('made_up_nonexistent_key', 'default_value') }} as col_value
