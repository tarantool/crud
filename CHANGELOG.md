# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2020-10-07

### Fixed

* Select with `==` conditions bugs
* Select with conditions by fields with collations

### Added

* CRUD operations:
 * replace
 * upsert
* Output format for CRUD operations changed to set of rows and metadata
* Insert/replace/upsert methods now accept tuples.
  To process unflattened objects *_object methods are introduced.
* `pairs` accepts `use_tomap` flag to return tuples or objects

### Changed

* `checks` is disabled for internal functions by default
* `limit` option is renamed to `first`
* Reverse pagination (negative `first`) is supported
* `after` option accepts a tuple

## [0.1.0] - 2020-09-23

### Added

* Basic CRUD operations:
  * insert
  * get
  * select
  * update
  * delete
* `pairs` function to iterate across the distributed space
