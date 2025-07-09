# Changelog

Notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - XXXX-XX-XX

### Added

- Functionality to convert an archive of netCDF files to csv files (util.archive_to_csv)

### Removed

- No longer accepts public or private outputs. These should be specified in separate "parent" repositories

### Changed

- AGAGE data specification is now removed. These files should be put in a different repository that calls the functions in this package. See https://github.com/AGAGE-atmosphere/agage-archive-template
- All files now contain almost the same variables (e.g., instrument_type, even if there is only one instrument)
- A release schedule is required for all instruments now. Previously missing from GCMS-Magnum and GCMS-Medusa-flask instruments.

### Fixed

- ```instrument_type``` variable no longer being removed if multiple instruments of the same type are combined (e.g., multiple Picarros)
  

## [0.1] - 2025-01-23

Initial release