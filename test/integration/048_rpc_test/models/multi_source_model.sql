select * from {{ source('test_source', 'other_test_table')}}
	join {{ source('other_source', 'test_table')}} using (id)
