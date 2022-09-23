# not strictly necessary, but this reflects the integration tests currently in the 'dbt-metrics' package right now
# i'm including just the first 10 rows for more concise 'git diff'

mock_purchase_data_csv = """purchased_at,payment_type,payment_total
2021-02-14 17:52:36,maestro,2418.94
2021-02-15 04:16:50,jcb,3043.28
2021-02-15 11:30:45,solo,1505.81
2021-02-16 13:08:18,,1532.85
2021-02-17 05:41:34,americanexpress,319.91
2021-02-18 06:47:32,jcb,2143.44
2021-02-19 01:37:09,jcb,840.1
2021-02-19 03:38:49,jcb,1388.18
2021-02-19 04:22:41,jcb,2834.96
2021-02-19 13:28:50,china-unionpay,2440.98
""".strip()

models__people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""
