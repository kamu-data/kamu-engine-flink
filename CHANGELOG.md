# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.18.2] - 2024-04-13
### Changed
- Less verbose default logging level
- Updated Rust toolchain and minor dependencies

## [0.18.1] - 2024-02-11
### Added
- Building a multi-platform image for `arm64` architectures

## [0.18.0] - 2024-01-10
### Changed
- Upgraded to unified ODF schema
- Prototyped corrections and retractions

## [0.17.0] - 2024-01-03
### Changed
- Upgraded to new ODF engine protocol

## [0.16.0] - 2024-01-03
### Changed
- Upgraded to new ODF schemas

## [0.15.1] - 2023-01-30
### Changed
- Use `FileSystemCheckpointStorage` to avoid errors when savepointing operators with state larger than 5MiB

## [0.15.0] - 2023-01-25
### Changed
- Massive: Upgrade to Flink 1.16

## [0.14.0] - 2022-04-22
### Changed
- Updated to latest ODF schemas
- Adapter will now tar/un-tar checkpoints to have them managed as files

> TODO: Some change logs are missing

## [0.6.0] - 2020-07-12
### Changed
- Upgraded to ODF manifests

## [0.5.0] - 2020-06-28
### Added
- Better Windows support

## [0.4.0] - 2020-06-28
### Changed
- Minimizing use of Hadoop FS

## [0.3.3] - 2020-06-26
### Fixed
- Avoid fixed-size field name collisions in Avro

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
- Initial version
