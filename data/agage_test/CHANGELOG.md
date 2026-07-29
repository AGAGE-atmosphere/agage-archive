# Changelog

Changes to the `agage_test` fixture archive. This is a test fixture, not a released
data product; the version is pinned to `testv1` in `data/agage_test/attributes.json`.

## [testv1]

- Release schedules trimmed so that every live cell has input data behind it, making a
  full `run_all` produce a completely determined archive with no error log.
- `README.md` and `CHANGELOG.md` added so that `copy_to_archive` is exercised.
