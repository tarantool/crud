# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

* Fixed error for partial result option if field contains box.NULL.

### Added

* `cut_rows` and `cut_objects` functions to cut off scan key and
  primary key values that were merged to the select/pairs partial result.
* Functions ``stop()`` for the roles ``crud-storage`` and ``crud-router``.
* Option flag `block_bucket_id_computation` for `select()`/`pairs()` to disable the `bucket_id` computation from primary key.

## [0.6.0] - 2021-03-29

### Fixed

* Fixed not finding field in tuple on `crud.update` if
  there are `is_nullable` fields in front of it that were added
  when the schema was changed for Tarantool version <= 2.2.

* Pagination over multipart primary key.

### Added

* `mode`, `prefer_replica` and `balance` options for read operations
  (get, select, pairs). According to this parameters one of vshard
  calls (`callrw`, `callro`, `callbro`, `callre`, `callbre`) is selected

## [0.5.0] - 2021-03-10

### Fixed

* Fixed not finding field in tuple on ``crud.update`` if
  there are ``is_nullable`` fields in front of it that were added
  when the schema was changed.
* Fixed select crash when dropping indexes
* Using outdated schema on router-side
* Sparsed tuples generation that led to "Tuple/Key must be MsgPack array" error

### Added

* Support for UUID field types and UUID values
* `fields` option for simple operations and select/pairs calls with pagination
  support to get partial result

## [0.4.0] - 2020-12-02

### Fixed

* Fixed typo in error for case when failed to get `bucket_id`
* Fixed select by part of sharding key equal. Before this patch
  selecting by equality of partially specified multipart primary index
  value was misinterpreted as a selecting by fully specified key value.
* Fixed iteration with `pairs` through empty space returned `nil`.

### Added

* `truncate` operation
* iterator returned by `pairs` is compatible with luafun

## [0.3.0] - 2020-10-26

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
