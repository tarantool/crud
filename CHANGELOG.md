# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2020-10-26

* Fix typo in error format. Now returned error contains parent error

### Fixed

* Select by primary index name
* Fix error handling select with invalid type value
* Get rid of performing map-reduce for single-replicaset operations

### Added

* `crud-router` Cartridge role
* `bucket_id` option for all operations to specify custom bucket ID.
  For operations that accepts tuple/object bucket ID can be specified as
  tuple/object field as well as `opts.bucket_id` value.

### Changed

* CRUD-router functions are exposed to the global scope, so it's possible to call
  crud-operations via `net.box.call`
* `crud.init` is removed in favor to `crud.init_storage` and `crud.init_router`

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
