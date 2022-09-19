
## Base adapters

### impl.py

The class `SQLAdapter` in [base/imply.py](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/adapters/base/impl.py) is a (mostly) abstract object that adapter objects inherit from. The base class scaffolds out methods that every adapter project usually should implement for smooth communication between dbt and database.

Some target databases require more or fewer methods--it all depends on what the warehouse's featureset is.

Look into the class for function-level comments.
