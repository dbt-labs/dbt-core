### Fixes

- Fix retry task failure caused by stale unique_id values when hashes change between runs. RetryTask now resolves logical node identity using package and name prefix before graph selection.
