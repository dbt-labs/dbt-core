
This test warrants some explanation. In dbt <=0.10.1, Redshift table and view materializations suffered from issues around concurrent transactions. In order to reliably reproduce this error, a query needs to select from a dbt model as the table is being rebuilt. Critically, this concurrent select needs to query the table during the drop/swap portition of the materialization. This looks like:

```sql
begin;
create table as (...);
drop table old_table cascade;
// <---- The concurrent query needs to be running here!
alter table new_table rename to old_table;
commit;
```

In order to reliably reproduce this failure, the model shown above needs to block for a long time between the `drop` and `alter` statements. We can't just stick a sleep() call in there, as this code is defined in the materialization. Instead, we can reliably reproduce the failure by:

1) creating a view that depends on this model
2) issuing a long-running query on the view before `dbt run` is invoked
3) issuing _another_ long-running query against the original model

Since long-running query (step 2) is selecting from the view, Redshift blocks on the `drop ... cascade`, of the materialization, which causes the query from step 3 time to overlap with the critical section of the materialization between the `drop` and `alter` statements.

In dbt v0.10.1, this integration test results in:

```
======================================================================
FAIL: test__redshift__concurrent_transaction (test_concurrent_transaction.TestConcurrentTransaction)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "/usr/src/app/test/integration/032_concurrent_transaction_test/test_concurrent_transaction.py", line 84, in test__redshift__concurrent_transaction
    self.assertEqual(self.query_state['model_1'], 'good')
AssertionError: 'error: table 3379442 dropped by concurrent transaction\n' != 'good'
- error: table 3379442 dropped by concurrent transaction
+ good
```
