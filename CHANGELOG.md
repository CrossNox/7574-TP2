# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2022-05-25
### Added
- CLI command to render DAG plot and create docker compose

## [2.0.0] - 2022-05-25
### Added
- DAG submodule that helps in the definition of the DAG
- DAG nodes for ventilator pattern, workers, joiners, sources and sinks
- DAG object which renders a graphviz plot of itself and dumps to a docker compose yaml
- Makefile to build docker image
- Poison pill ACK protocol

## Fixed
- Dockerfile was fixed to take less building time
- Race condition on dependencies solving for workers. This halted all execution given
  that no ACKs were issued to upstream publishers.
- Bug in logic for `FilterNanSentiment`
- Bug in logic for `FilterNullUrl`
- Inconsistencies between expected `id` keys and `post_id` received
- Bug in sum of `MeanSentiment`

## Changed
- The `nsubs` CLI option for filters and transform is now mandatory and has no default
- Arguments passed to sub-commands are no longer at the sub-command level and
  are now at the sub-sub-command level
- High Water Mark for pub/sub explicitly set to zero
- Joiners now need to receive a socket to ACK poison pills
- `FilterUniqPosts` now returns several scalar messages instead of array


## [1.7.0] - 2022-05-24
### Added
- Top post download sink

## [1.6.0] - 2022-05-24
### Added
- Missing CLI interfaces for transforms

## [1.5.0] - 2022-05-24
### Added
- Missing CLI interfaces for sinks

## [1.4.0] - 2022-05-24
### Added
- Missing CLI interfaces for filters

## [1.3.0] - 2022-05-24
### Added
- Join CLI

## [1.2.0] - 2022-05-23
### Added
- Joiners

## [1.1.0] - 2022-05-23
### Added
- Single worker executor with dependencies
- DAG tests
- Sink to file

## [1.0.0] - 2022-05-23
### Changed
- Restructured project
- Refactored filters and transforms under common interface
- Abstracted ventilator components
- Updated CLIs

## [0.10.0] - 2022-05-22
### Added
- Filter posts above mean score

## [0.9.0] - 2022-05-22
### Added
- Calculate posts mean score

## [0.8.0] - 2022-05-22
### Added
- Filter null url posts

## [0.7.0] - 2022-05-22
### Added
- Filter columns

## [0.6.0] - 2022-05-22
### Added
- Filter unique posts

## [0.5.0] - 2022-05-22
### Added
- Calculate mean sentiment

## [0.4.0] - 2022-05-22
### Added
- Filter by nan sentiment

## [0.3.0] - 2022-05-22
### Added
- Transformer to extract post id

## [0.2.0] - 2022-05-22
### Added
- Base structure for DAG elements
- Base CLI structure for sources, filters and sinks

## [0.1.0] - 2022-05-12
### Added
- Initial commit with basic structure
