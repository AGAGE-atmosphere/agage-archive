# Changelog

Notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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