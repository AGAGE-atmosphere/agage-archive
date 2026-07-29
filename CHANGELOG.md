# Changelog

Notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **The archive is now reproducible.** Two sources of run-to-run variation have been removed. Both could change the contents of a published archive between otherwise identical runs, so output may differ from previous versions — but it will now be the same every time.
  - `read_data_combination` iterated `set(instruments)`, whose order depends on Python's hash seed. That order is carried through to `xr.concat` in `combine_datasets`, where `combine_attrs="override"` takes global attributes from whichever dataset happens to be first. Combined-file attributes such as `inlet_latitude`, `inlet_longitude`, `inlet_base_elevation_masl` and `inlet_comment` therefore varied between runs. Instrument order now follows the column order of the `data_combination` file, so the attributes come from the first instrument listed there.
  - `run_all` processed instruments in filesystem glob order. Where more than one instrument is eligible to write the top-level file for a species (because the species has one or no entry in `data_combination`), the first one processed wins, so which instrument's data ended up in the top-level file was not reproducible. Instruments are now processed in sorted order. Sites are likewise sorted.
- Processing an instrument with no baseline flags (currently `GCMS-Medusa-flask`) while `baseline=True` raised `TypeError: 'NoneType' object is not callable`, which was swallowed into `error_log_individual.txt`, so flask data produced no output at all. Baseline and monthly-baseline products are now skipped for such instruments and the mole fraction files are written as normal.
- The check that a baseline file has the same timestamps as its mole fraction file could never fire. `(ds_baseline.time != ds.time).any()` is an xarray comparison, which aligns both operands on the time coordinate before comparing, so every timestamp was compared with itself and the result was always `False` — for any input, including two datasets with no timestamps in common. It now compares the raw values. This was dead code in both `run_timestamp_checks` and `run_individual_site`; enabling it revealed no problems in existing data, but a genuine mismatch will now be caught instead of published.
- `run_timestamp_checks` tested an `xarray.Dataset` for truthiness, so an empty baseline dataset silently skipped the duplicate-timestamp and timestamp-alignment checks rather than failing them.

### Changed

- **Processing now fails when more than one instrument is eligible to write the top-level (recommended) file for a species.** This happens when a species is measured by several instruments at a site but has no row in that site's `data_combination` file: each instrument's individual file is then also written to `{species}/`, and the first one processed won, silently discarding the others. In the test fixture this was dropping about 80% of the recommended record for some species — Kennaook/Cape Grim `ccl4` contained 7,643 points of ALE data instead of 41,451 points of ALE and GAGE. Any species in this position must now be given an explicit `data_combination` row. Requires `data_combination` files to be complete; add a row for every species measured by more than one instrument at a site.
- Combined files now take their global attributes from the **most recently operating instrument**, rather than from whichever instrument happened to be first (#167). Previously these came from the first instrument listed in the `data_combination` file, which is normally the oldest, so a combined record could inherit station metadata from an instrument decommissioned decades earlier — for example Kennaook/Cape Grim `ch3ccl3` reported an empty `inlet_comment` because ALE had none, discarding the description carried by the current Medusa. Affects `inlet_latitude`, `inlet_longitude`, `inlet_base_elevation_masl` and `inlet_comment` on combined mole fraction, baseline and monthly-baseline files.
- An option has been added to the run functions to apply a filter to paired flask measurements. If ` flask_pair_agreement=True` is passed, pairs of measurements made at the same time will be filtered out if the pair difference is greater than twice the instrument precision (estimated as the `std_stdev`, the daily standard deviation of the measured standards). This is only relevant when processing flask measurements. 

### Added

- A golden manifest test (`tests/test_archive.py`) that runs the full `agage_test` archive and compares every file's path, variables, dtypes, encoding, attributes and data checksum against a checked-in reference, and asserts that no errors were written to the error logs. Regenerate the reference with `AGAGE_UPDATE_MANIFEST=1 python -m pytest tests/test_archive.py` when output changes intentionally.


## [0.2.1] - 2026-01-08

### Changed
- The delimeters in the filename for scale default files has been changed from a hyphen (-) to an underscore (_). So files should be named like: `scale_defaults_INSTRUMENT{_SITE}.csv` instead of `scale_defaults-INSTRUMENT{-SITE}.csv`. This is because some instrument names contain hyphens which caused issues when parsing the filenames.


## [0.2] - 2025-07-28

### Added

- Functionality to convert an archive of netCDF files to csv files (util.archive_to_csv)
- instrument_type is now taken directly from the filenames in data_release_schedule
- You must specify a data read/processing function for each instrument type in ```data/NETWORK/data_read_function.json```. Current functions are:
  - ```read_nc``` (reads GCWerks netcdf files)
  - ```read_gcwerks_flask``` (reads GCWerks flask data files, and possibly GCCompare input netcdf files?)
  - ```read_ale_gage``` (ALE/GAGE files in the Georgia Institute of Technology 1994 format)
  - ```read_gcms_magnum``` (reads the archived "Magnum" files at Mace Head. Unlikely to be used for any other format)

### Removed

- No longer accepts public or private outputs. These should be specified in separate "parent" repositories

### Changed

- AGAGE data specification is now removed. These files should be put in a different repository that calls the functions in this package. See https://github.com/AGAGE-atmosphere/agage-archive-template
- All files now contain almost the same variables (e.g., instrument_type, even if there is only one instrument)
- A release schedule is required for all instruments now. Previously missing from GCMS-Magnum and GCMS-Medusa-flask instruments.
- The instrument_type values are defined flexibly for each network. Instrument types are taken from the filename of the release schedule csv files.
- The ```config.yaml``` file now expects a path for every instrument_type defined in the release schedules

### Fixed

- ```instrument_type``` variable no longer being removed if multiple instruments of the same type are combined (e.g., multiple Picarros)
  

## [0.1] - 2025-01-23

Initial release