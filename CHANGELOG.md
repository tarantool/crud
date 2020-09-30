# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

* Select with `==` conditions bugs
* Select with conditions by fields with collations

### Added

* CRUD operations:
 * replace
 * upsert

## [0.1.0] - 2020-09-23

### Added

* Basic CRUD operations:
  * insert
  * get
  * select
  * update
  * delete
* `pairs` function to iterate across the distributed space
