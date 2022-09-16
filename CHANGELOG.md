# Changelog

## [2.2.0](https://github.com/ibis-project/ibis-bigquery/compare/v2.1.3...v2.2.0) (2022-09-16)


### Features

* Add difference and intersect  BigQuery classes ([#88](https://github.com/ibis-project/ibis-bigquery/issues/88)) ([d3acf50](https://github.com/ibis-project/ibis-bigquery/commit/d3acf50de9d8e9015819efe300b4b5f9615c027a))
* Add rewrite function for `ops.FloorDivide` ([#85](https://github.com/ibis-project/ibis-bigquery/issues/85)) ([a04a674](https://github.com/ibis-project/ibis-bigquery/commit/a04a6741d99e037aaadee3eafc913980a8ade134))
* **ibis_bigquery/registry.py:** add wiring for bigquery-supported ibis geospatial operations ([#143](https://github.com/ibis-project/ibis-bigquery/issues/143)) ([b35cda9](https://github.com/ibis-project/ibis-bigquery/commit/b35cda9705c1c75b8091ccfa3a07455279746690))
* partial support for integer to timestamp with nanosecond units ([#138](https://github.com/ibis-project/ibis-bigquery/issues/138)) ([e3997d4](https://github.com/ibis-project/ibis-bigquery/commit/e3997d42752ec49f4b8c8625097682b27ef4d350))


### Bug Fixes

* **compat:** fix failing approx methods against ibis `master` ([#142](https://github.com/ibis-project/ibis-bigquery/issues/142)) ([55a831a](https://github.com/ibis-project/ibis-bigquery/commit/55a831a43b0f28d70173db2e8989e4047769d653))
* **deps:** unconstrain pyarrow to support 8 and 9 ([#145](https://github.com/ibis-project/ibis-bigquery/issues/145)) ([3ada400](https://github.com/ibis-project/ibis-bigquery/commit/3ada400325681855f8afc1921039b50c350167a7))


### Dependencies

* require sqlalchemy for ibis SQL backend ([#144](https://github.com/ibis-project/ibis-bigquery/issues/144)) ([bb76554](https://github.com/ibis-project/ibis-bigquery/commit/bb765542f9b2bf31107ccd95563b6a0354f81898))

### [2.1.3](https://github.com/ibis-project/ibis-bigquery/compare/v2.1.2...v2.1.3) (2022-05-25)


### Bug Fixes

* ensure that ScalarParameter names are used instead of Alias names ([#135](https://github.com/ibis-project/ibis-bigquery/issues/135)) ([bfe539a](https://github.com/ibis-project/ibis-bigquery/commit/bfe539a7c60439f7a521e230736aab3961dbabcc))

### [2.1.2](https://github.com/ibis-project/ibis-bigquery/compare/v2.1.1...v2.1.2) (2022-04-26)


### Bug Fixes

* **udf:** use object.__setattr__ for 3.0.0 compatibility ([#122](https://github.com/ibis-project/ibis-bigquery/issues/122)) ([ec15188](https://github.com/ibis-project/ibis-bigquery/commit/ec151883d7f1e67e9b56725ceb81743970563115))


### Dependencies

* support google-cloud-bigquery 3.0 ([25fc69e](https://github.com/ibis-project/ibis-bigquery/commit/25fc69e11429bbb45f992a5db7bfb4e8615eb34b))
* support ibis 3.0 ([#124](https://github.com/ibis-project/ibis-bigquery/issues/124)) ([25fc69e](https://github.com/ibis-project/ibis-bigquery/commit/25fc69e11429bbb45f992a5db7bfb4e8615eb34b))

### [2.1.1](https://github.com/ibis-project/ibis-bigquery/compare/v2.1.0...v2.1.1) (2022-03-29)


### Bug Fixes

* update to UDFContext for trans_numeric_udf function ([#119](https://github.com/ibis-project/ibis-bigquery/issues/119)) ([daf4da1](https://github.com/ibis-project/ibis-bigquery/commit/daf4da1c1dc2e1002570ff86cc358400d7f6832d))

## [2.1.0](https://github.com/ibis-project/ibis-bigquery/compare/v2.0.0...v2.1.0) (2022-03-15)


### Features

* Raise better error message when incorrect dataset is supplied [#113](https://github.com/ibis-project/ibis-bigquery/issues/113) ([#115](https://github.com/ibis-project/ibis-bigquery/issues/115)) ([dc474af](https://github.com/ibis-project/ibis-bigquery/commit/dc474af94bb8590c9acf3ec3f94634f366349580))


### Bug Fixes

* avoid deprecated "out-of-band" authentication flow ([#116](https://github.com/ibis-project/ibis-bigquery/issues/116)) ([9dc5808](https://github.com/ibis-project/ibis-bigquery/commit/9dc580800d607b809433bb2a3f2da2ba43b2f679))


### Dependencies

* fix minimum ibis-framework dependency ([b8834ce](https://github.com/ibis-project/ibis-bigquery/commit/b8834ce58453a09d790f44eb73f98319f17f84fa))

## [2.0.0](https://www.github.com/ibis-project/ibis-bigquery/compare/v0.1.1...v2.0.0) (2021-12-02)


### ⚠ BREAKING CHANGES

* support ibis 2.x, drop ibis 1.x (#93)

### Features

* support ibis 2.x, drop ibis 1.x ([#93](https://www.github.com/ibis-project/ibis-bigquery/issues/93)) ([780d071](https://www.github.com/ibis-project/ibis-bigquery/commit/780d07168758571d582e8a679e194ac8de33b36b))


### Miscellaneous Chores

* release 2.0.0 ([c5c3f24](https://www.github.com/ibis-project/ibis-bigquery/commit/c5c3f2414dbb2046b5e3bdb14204b6440c9a772b))

## [1.0.0](https://www.github.com/ibis-project/ibis-bigquery/compare/v0.1.1...v1.0.0) (2021-12-02)

### Bug Fixes

* substr fails to compile ([#95](https://github.com/ibis-project/ibis-bigquery/pull/95))) 

## [0.1.1](https://www.github.com/ibis-project/ibis-bigquery/compare/v0.1.0...v0.1.1) (2021-04-28)


### Dependencies

* support pyarrow 4 ([#45](https://www.github.com/ibis-project/ibis-bigquery/issues/45)) ([0346821](https://www.github.com/ibis-project/ibis-bigquery/commit/03468217650d639d304c91e00ca4507828cfcfc4))

## 0.1.0 (2021-04-27)


### Features

* add `entry_points` so that ibis 2 can discover this backend ([#38](https://www.github.com/ibis-project/ibis-bigquery/issues/38)) ([c3d188e](https://www.github.com/ibis-project/ibis-bigquery/commit/c3d188e107176c7fff6e7bce572330797cb3e2bc))
* add `ibis_bigquery.__version__` property ([#29](https://www.github.com/ibis-project/ibis-bigquery/issues/29)) ([58d624a](https://www.github.com/ibis-project/ibis-bigquery/commit/58d624abaaa9db4106241128559e28b5c2a2e715))
* add `ibis_bigquery.connect` and `ibis_bigquery.compile` functions ([#37](https://www.github.com/ibis-project/ibis-bigquery/issues/37)) ([7348bf2](https://www.github.com/ibis-project/ibis-bigquery/commit/7348bf2daea0f99e0e46d77cdcd8863f4274ab8b))
* check for negative values before doing substr ([#32](https://github.com/ibis-project/ibis-bigquery/pull/32)) ([d515184](https://github.com/ibis-project/ibis-bigquery/commit/d51518427b3178939ff40fd6a62f8298e71b57a0))


### Bug Fixes

* compatibility with ibis 1.4.0 (and possibly 1.2, 1.3) ([#31](https://www.github.com/ibis-project/ibis-bigquery/issues/31)) ([b6bbfbe](https://www.github.com/ibis-project/ibis-bigquery/commit/b6bbfbe412ec017e441ecb730c590dfccadfbd84))
* update UDF to support Python 3.8+ AST ([#25](https://www.github.com/ibis-project/ibis-bigquery/issues/25)) ([3d9b2cb](https://www.github.com/ibis-project/ibis-bigquery/commit/3d9b2cbda4ea091bfa1442a306c4ef9271fb3a4c))
* use TIMESTAMP_SUB for TimstampSub operation ([#40](https://www.github.com/ibis-project/ibis-bigquery/issues/40)) ([4c5cb57](https://www.github.com/ibis-project/ibis-bigquery/commit/4c5cb5769497eece2913ec567057b6d440c0922b))
