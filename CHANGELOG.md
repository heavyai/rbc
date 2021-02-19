
# What's New

## **RBC 0.6.0** (unreleased)

#### Changes
* Add device annotation to `external` ([gh-275](https://github.com/xnd-project/rbc/pull/275))
* Change `external` to work with hardware specific types ([gh-273](https://github.com/xnd-project/rbc/pull/273))

#### Deprecations
* rbc no longer supports Python 3.6 ([gh-279](https://github.com/xnd-project/rbc/pull/279))

#### New Features
* Introduce `TargetInfo.dummy()` ([gh-274](https://github.com/xnd-project/rbc/pull/274))
* Support C function prototypes, add `Type.name` property to function and fix `Type.is_function` ([gh-269](https://github.com/xnd-project/rbc/pull/269))
* Add command to declare external functions ([gh-268](https://github.com/xnd-project/rbc/pull/268))
* Introduce ccompiler, Array.ptr and a prototype of treelite prediction model as an UDF ([gh-267](https://github.com/xnd-project/rbc/pull/267))

#### Bug Fixes

#### Documentation
* Fix `External.fromobject` docstring([gh-272](https://github.com/xnd-project/rbc/pull/272))


#### Internal Changes
* Replace Travis CI by github actions ([gh-279](https://github.com/xnd-project/rbc/pull/279))
* Move `IS_GPU` and `IS_CPU` to `irtools.py` ([gh-281](https://github.com/xnd-project/rbc/pull/281))


------

## **RBC 0.5.1** - [2021/01/29](https://github.com/xnd-project/rbc/releases/tag/v0.5.1)

#### Changes

#### Deprecations

#### New Features
* Add `Array.is_null` and `Array.is_null(index)` ([gh-244](https://github.com/xnd-project/rbc/pull/244))

#### Bug Fixes
* Fix setting array null values to None. Add tests to test tables. Support NULLs in table_load_columnar ([gh-260](https://github.com/xnd-project/rbc/pull/260))
* Ignore cpu features due to llvm version mismatch ([gh-253](https://github.com/xnd-project/rbc/pull/253))

#### Documentation

#### Internal Changes
* Replace Circle CI and AppVeyor by GitHub Actions ([gh-256](https://github.com/xnd-project/rbc/pull/257))
* Release RBC to PyPI automatically using GitHub Actions ([gh-249](https://github.com/xnd-project/rbc/pull/249), [gh-248](https://github.com/xnd-project/rbc/pull/248), [gh-247](https://github.com/xnd-project/rbc/pull/247))


-----

## **RBC 0.5.0** - [2021/01/13](https://github.com/xnd-project/rbc/releases/tag/v0.5.0)

#### Changes

#### Deprecations

#### New Features
* Add `Array.is_null` and `Array.is_null(index)` ([gh-244](https://github.com/xnd-project/rbc/pull/244))
* Implement template support and make custom type a fundamental type of RBC typesystem ([gh-241](https://github.com/xnd-project/rbc/pull/241))
* Add `Column.is_null` ([gh-230](https://github.com/xnd-project/rbc/pull/230)/[gh-239](https://github.com/xnd-project/rbc/pull/230))
* Add `bitwise_not` implementation ([gh-226](https://github.com/xnd-project/rbc/pull/226))
* Support TEXT ENCODING NONE as Bytes ([gh-225](https://github.com/xnd-project/rbc/pull/225))
* Handle definitions that cannot be run on GPU device ([gh-209](https://github.com/xnd-project/rbc/pull/209))
* Add libdevice bindings for Numpy math functions ([gh-204](https://github.com/xnd-project/rbc/pull/204))
* Support Constant and ConstantParameter sizers ([gh-201](https://github.com/xnd-project/rbc/pull/201))
* Add libdevice to Omniscidb ([gh-198](https://github.com/xnd-project/rbc/pull/198))
* Support multiple columns in a single cursor argument of a UDTF. ([gh-195](https://github.com/xnd-project/rbc/pull/195))

#### Bug Fixes
* Fix adding two int32 values results in int64, which is wrong ([gh-214](https://github.com/xnd-project/rbc/pull/214))
* Restore `allocate_varlen_buffer` ([gh-236](https://github.com/xnd-project/rbc/pull/236))

#### Documentation

#### Internal Changes
* Replace min version from 5.5 to 5.6 in some tests ([gh-246](https://github.com/xnd-project/rbc/pull/246))
* Replace miniconda by minimamba on Circle CI ([gh-243](https://github.com/xnd-project/rbc/pull/243))
* Tests for omniscidb-internal PR 5134 - fix multiple cursor argument support for UDTFs ([gh-238](https://github.com/xnd-project/rbc/pull/238))
* Add UDTF overload tests for uniform signatures ([gh-234](https://github.com/xnd-project/rbc/pull/234))
* Add test for columns with different sizes as input to UDTFs ([gh-231](https://github.com/xnd-project/rbc/pull/231))
* Implement tests for omniscidb-internal PR 5026 ([gh-229](https://github.com/xnd-project/rbc/pull/229))
* Introduce `TargetInfo.check_enabled` and `RemoteJIT.preprocess_callable` ([gh-228](https://github.com/xnd-project/rbc/pull/228))
* Introduce `TargetInfo.llvm_version` property ([gh-227](https://github.com/xnd-project/rbc/pull/227))
* Make TargetInfo globally available ([gh-219](https://github.com/xnd-project/rbc/pull/219))
* Tests for aggregates on UDTF output columns ([gh-212](https://github.com/xnd-project/rbc/pull/212))
* Enable test_black_scholes_udtf ([gh-210](https://github.com/xnd-project/rbc/pull/210))
* Eliminate internal dependency on pymapd ([gh-206](https://github.com/xnd-project/rbc/pull/206))
* Tests for table function overload/redefine support ([gh-199](https://github.com/xnd-project/rbc/pull/199))

