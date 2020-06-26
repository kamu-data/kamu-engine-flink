# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2] - 2020-06-26
### Fixed
- Using `fixed_len_byte_array` instead of `binary` when writing `DECIMAL` to Parquet

## [0.3.1] - 2020-06-26
### Fixed
- Using latest `1.12-SNAPSHOT` build
- Above preserves the precision/scale of `DECIMAL` types that are read from Parquet

## [0.3.0] - 2020-06-25
### Fixed
- Using a `1.12-SNAPSHOT` build from https://github.com/apache/flink/pull/12768
- Above fixes the problem of reading `DECIMAL` type from Parquet

## [0.2.0] - 2020-06-23
### Added
- Persisting watermarks between restarts
- Support for manual watermarks

## [0.1.0] - 2020-06-14
### Added
- Initial verison
