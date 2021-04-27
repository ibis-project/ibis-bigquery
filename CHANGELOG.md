# Changelog

## 0.1.0 (2021-04-27)


### Features

* add `entry_points` so that ibis 2 can discover this backend ([#38](https://www.github.com/ibis-project/ibis-bigquery/issues/38)) ([c3d188e](https://www.github.com/ibis-project/ibis-bigquery/commit/c3d188e107176c7fff6e7bce572330797cb3e2bc))
* add `ibis_bigquery.__version__` property ([#29](https://www.github.com/ibis-project/ibis-bigquery/issues/29)) ([58d624a](https://www.github.com/ibis-project/ibis-bigquery/commit/58d624abaaa9db4106241128559e28b5c2a2e715))
* add `ibis_bigquery.connect` and `ibis_bigquery.compile` functions ([#37](https://www.github.com/ibis-project/ibis-bigquery/issues/37)) ([7348bf2](https://www.github.com/ibis-project/ibis-bigquery/commit/7348bf2daea0f99e0e46d77cdcd8863f4274ab8b))


### Bug Fixes

* compatibility with ibis 1.4.0 (and possibly 1.2, 1.3) ([#31](https://www.github.com/ibis-project/ibis-bigquery/issues/31)) ([b6bbfbe](https://www.github.com/ibis-project/ibis-bigquery/commit/b6bbfbe412ec017e441ecb730c590dfccadfbd84))
* update UDF to support Python 3.8+ AST ([#25](https://www.github.com/ibis-project/ibis-bigquery/issues/25)) ([3d9b2cb](https://www.github.com/ibis-project/ibis-bigquery/commit/3d9b2cbda4ea091bfa1442a306c4ef9271fb3a4c))
* use TIMESTAMP_SUB for TimstampSub operation ([#40](https://www.github.com/ibis-project/ibis-bigquery/issues/40)) ([4c5cb57](https://www.github.com/ibis-project/ibis-bigquery/commit/4c5cb5769497eece2913ec567057b6d440c0922b))