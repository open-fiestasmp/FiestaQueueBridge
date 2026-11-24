# Changelog

## Unreleased

- Merge each edge heartbeat and queue snapshot into one coordinator transaction.
- Read queue snapshots without taking a SQLite write lock.
- Flush coordinator request logs as they are written and include SQLite's safe
  error text in request-failure logs.
- Use a four-second request timeout and two-second poll interval in the edge
  defaults and example configuration.
- Cover concurrent refreshes and expired queue claims in the Python suite.
