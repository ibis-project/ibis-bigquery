import datetime

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import packaging.version
import pandas as pd
import pytest
from ibis.expr.types import TableExpr
from pytest import param

import ibis_bigquery

IBIS_VERSION = packaging.version.Version(ibis.__version__)
IBIS_1_4_VERSION = packaging.version.Version("1.4.0")
IBIS_3_0_VERSION = packaging.version.Version("3.0.0")
IBIS_3_0_2_VERSION = packaging.version.Version("3.0.2")
IBIS_3_1_0_VERSION = packaging.version.Version("3.1.0")


@pytest.mark.parametrize(
    ("case", "expected", "dtype"),
    [
        (datetime.date(2017, 1, 1), "DATE '2017-01-01'", dt.date),
        (
            pd.Timestamp("2017-01-01"),
            "DATE '2017-01-01'",
            dt.date,
        ),
        ("2017-01-01", "DATE '2017-01-01'", dt.date),
        (
            datetime.datetime(2017, 1, 1, 4, 55, 59),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        (
            "2017-01-01 04:55:59",
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        (
            pd.Timestamp("2017-01-01 04:55:59"),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
    ],
)
def test_literal_date(case, expected, dtype):
    expr = ibis.literal(case, type=dtype).year()
    result = ibis_bigquery.compile(expr)
    expected_name = "tmp" if IBIS_VERSION <= IBIS_3_0_2_VERSION else "year"
    assert result == f"SELECT EXTRACT(year from {expected}) AS `{expected_name}`"


@pytest.mark.parametrize(
    ("case", "expected", "dtype", "strftime_func"),
    [
        (
            datetime.date(2017, 1, 1),
            "DATE '2017-01-01'",
            dt.date,
            "FORMAT_DATE",
        ),
        (
            pd.Timestamp("2017-01-01"),
            "DATE '2017-01-01'",
            dt.date,
            "FORMAT_DATE",
        ),
        (
            "2017-01-01",
            "DATE '2017-01-01'",
            dt.date,
            "FORMAT_DATE",
        ),
        (
            datetime.datetime(2017, 1, 1, 4, 55, 59),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
            "FORMAT_TIMESTAMP",
        ),
        (
            "2017-01-01 04:55:59",
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
            "FORMAT_TIMESTAMP",
        ),
        (
            pd.Timestamp("2017-01-01 04:55:59"),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
            "FORMAT_TIMESTAMP",
        ),
    ],
)
def test_day_of_week(case, expected, dtype, strftime_func):
    date_var = ibis.literal(case, type=dtype)
    expr_index = date_var.day_of_week.index()
    result = ibis_bigquery.compile(expr_index)
    assert result == f"SELECT MOD(EXTRACT(DAYOFWEEK FROM {expected}) + 5, 7) AS `tmp`"

    expr_name = date_var.day_of_week.full_name()
    result = ibis_bigquery.compile(expr_name)
    if strftime_func == "FORMAT_TIMESTAMP":
        assert result == f"SELECT {strftime_func}('%A', {expected}, 'UTC') AS `tmp`"
    else:
        assert result == f"SELECT {strftime_func}('%A', {expected}) AS `tmp`"


@pytest.mark.parametrize(
    ("case", "expected", "dtype"),
    [
        (
            "test of hash",
            "'test of hash'",
            dt.string,
        ),
        (
            b"test of hash",
            "FROM_BASE64('dGVzdCBvZiBoYXNo')",
            dt.binary,
        ),
    ],
)
def test_hash(case, expected, dtype):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    string_var = ibis.literal(case, type=dtype)
    expr = string_var.hash(how="farm_fingerprint")
    result = ibis_bigquery.compile(expr)
    assert result == f"SELECT farm_fingerprint({expected}) AS `tmp`"


@pytest.mark.parametrize(
    ("case", "expected", "how", "dtype"),
    [
        (
            "test",
            "md5('test')",
            "md5",
            dt.string,
        ),
        (
            b"test",
            "md5(FROM_BASE64('dGVzdA=='))",
            "md5",
            dt.binary,
        ),
        (
            "test",
            "sha1('test')",
            "sha1",
            dt.string,
        ),
        (
            b"test",
            "sha1(FROM_BASE64('dGVzdA=='))",
            "sha1",
            dt.binary,
        ),
        (
            "test",
            "sha256('test')",
            "sha256",
            dt.string,
        ),
        (
            b"test",
            "sha256(FROM_BASE64('dGVzdA=='))",
            "sha256",
            dt.binary,
        ),
        (
            "test",
            "sha512('test')",
            "sha512",
            dt.string,
        ),
        (
            b"test",
            "sha512(FROM_BASE64('dGVzdA=='))",
            "sha512",
            dt.binary,
        ),
    ],
)
def test_hashbytes(case, expected, how, dtype):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    var = ibis.literal(case, type=dtype)
    expr = var.hashbytes(how=how)
    result = ibis_bigquery.compile(expr)
    assert result == f"SELECT {expected} AS `tmp`"


@pytest.mark.parametrize(
    ("case", "unit", "expected"),
    (
        (
            123456789,
            "s",
            "TIMESTAMP_SECONDS(123456789)",
        ),
        (
            -123456789,
            "ms",
            "TIMESTAMP_MILLIS(-123456789)",
        ),
        (
            123456789,
            "us",
            "TIMESTAMP_MICROS(123456789)",
        ),
        (
            1234567891011,
            "ns",
            "TIMESTAMP_MICROS(CAST(ROUND(1234567891011 / 1000) AS INT64))",
        ),
    ),
)
def test_integer_to_timestamp(case, unit, expected):
    expr = ibis.literal(case, type=dt.int64).to_timestamp(unit=unit)
    result = ibis_bigquery.compile(expr)
    assert result == f"SELECT {expected} AS `tmp`"


@pytest.mark.parametrize(
    ("case", "expected", "dtype"),
    [
        (
            datetime.datetime(2017, 1, 1, 4, 55, 59),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        (
            "2017-01-01 04:55:59",
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        (
            pd.Timestamp("2017-01-01 04:55:59"),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        (datetime.time(4, 55, 59), "TIME '04:55:59'", dt.time),
        ("04:55:59", "TIME '04:55:59'", dt.time),
    ],
)
def test_literal_timestamp_or_time(case, expected, dtype):
    expr = ibis.literal(case, type=dtype).hour()
    result = ibis_bigquery.compile(expr)
    expected_name = "tmp" if IBIS_VERSION <= IBIS_3_0_2_VERSION else "hour"
    assert result == f"SELECT EXTRACT(hour from {expected}) AS `{expected_name}`"


@pytest.mark.parametrize(
    "expected",
    [
        param(
            """\
WITH t0 AS (
  SELECT *
  FROM unbound_table
  WHERE `PARTITIONTIME` < DATE '2017-01-01'
),
t1 AS (
  SELECT CAST(`file_date` AS DATE) AS `file_date`, `PARTITIONTIME`, `val`
  FROM t0
),
t2 AS (
  SELECT t1.*
  FROM t1
  WHERE t1.`file_date` < DATE '2017-01-01'
),
t3 AS (
  SELECT *, `val` * 2 AS `XYZ`
  FROM t2
)
SELECT t3.*
FROM t3
  INNER JOIN t3 t4""",
            marks=[
                pytest.mark.xfail(
                    IBIS_VERSION > IBIS_3_1_0_VERSION,
                    reason="ibis is older than or equal to 3.1.0",
                )
            ],
            id="older_than_or_equal_to_31",
        ),
        param(
            """\
WITH t0 AS (
  SELECT *
  FROM unbound_table
  WHERE `PARTITIONTIME` < DATE '2017-01-01'
),
t1 AS (
  SELECT CAST(`file_date` AS DATE) AS `file_date`, `PARTITIONTIME`, `val`
  FROM t0
  WHERE `file_date` < DATE '2017-01-01'
),
t2 AS (
  SELECT *, `val` * 2 AS `XYZ`
  FROM t1
)
SELECT t2.*
FROM t2
  INNER JOIN t2 t3""",
            marks=[
                pytest.mark.xfail(
                    IBIS_VERSION <= IBIS_3_1_0_VERSION,
                    reason="ibis is newer than 3.1.0",
                )
            ],
            id="newer_than_31",
        ),
    ],
)
@pytest.mark.skipif(IBIS_VERSION < IBIS_1_4_VERSION, reason="requires ibis 1.4+")
def test_projection_fusion_only_peeks_at_immediate_parent(expected):
    schema = [
        ("file_date", "timestamp"),
        ("PARTITIONTIME", "date"),
        ("val", "int64"),
    ]
    table = ibis.table(schema, name="unbound_table")
    table = table[table.PARTITIONTIME < ibis.date("2017-01-01")]
    table = table.mutate(file_date=table.file_date.cast("date"))
    table = table[table.file_date < ibis.date("2017-01-01")]
    table = table.mutate(XYZ=table.val * 2)
    expr = table.join(table.view())[table]
    result = ibis_bigquery.compile(expr)
    assert result == expected


@pytest.mark.parametrize(
    ("unit", "expected_unit", "expected_func"),
    [
        ("Y", "YEAR", "TIMESTAMP"),
        ("Q", "QUARTER", "TIMESTAMP"),
        ("M", "MONTH", "TIMESTAMP"),
        ("W", "WEEK", "TIMESTAMP"),
        ("D", "DAY", "TIMESTAMP"),
        ("h", "HOUR", "TIMESTAMP"),
        ("m", "MINUTE", "TIMESTAMP"),
        ("s", "SECOND", "TIMESTAMP"),
        ("ms", "MILLISECOND", "TIMESTAMP"),
        ("us", "MICROSECOND", "TIMESTAMP"),
        ("Y", "YEAR", "DATE"),
        ("Q", "QUARTER", "DATE"),
        ("M", "MONTH", "DATE"),
        ("W", "WEEK", "DATE"),
        ("D", "DAY", "DATE"),
        ("h", "HOUR", "TIME"),
        ("m", "MINUTE", "TIME"),
        ("s", "SECOND", "TIME"),
        ("ms", "MILLISECOND", "TIME"),
        ("us", "MICROSECOND", "TIME"),
    ],
)
def test_temporal_truncate(unit, expected_unit, expected_func):
    t = ibis.table([("a", getattr(dt, expected_func.lower()))], name="t")
    expr = t.a.truncate(unit)
    result = ibis_bigquery.compile(expr)
    expected = f"""\
SELECT {expected_func}_TRUNC(`a`, {expected_unit}) AS `tmp`
FROM t"""
    assert result == expected


@pytest.mark.parametrize("kind", ["date", "time"])
def test_extract_temporal_from_timestamp(kind):
    t = ibis.table([("ts", dt.timestamp)], name="t")
    expr = getattr(t.ts, kind)()
    result = ibis_bigquery.compile(expr)
    expected = f"""\
SELECT {kind.upper()}(`ts`) AS `tmp`
FROM t"""
    assert result == expected


def test_now():
    expr = ibis.now()
    result = ibis_bigquery.compile(expr)
    expected = "SELECT CURRENT_TIMESTAMP() AS `tmp`"
    assert result == expected


def test_binary():
    t = ibis.table([("value", "double")], name="t")
    expr = t["value"].cast(dt.binary).name("value_hash")
    result = ibis_bigquery.compile(expr)
    expected_name = "tmp" if IBIS_VERSION < IBIS_3_0_VERSION else "value_hash"
    expected = f"""\
SELECT CAST(`value` AS BYTES) AS `{expected_name}`
FROM t"""
    assert result == expected


def test_substring():
    t = ibis.table([("value", "string")], name="t")
    expr = t["value"].substr(3, 1)
    expected = """\
SELECT substr(`value`, 3 + 1, 1) AS `tmp`
FROM t"""
    result = ibis_bigquery.compile(expr)

    assert result == expected


def test_substring_neg_length():
    t = ibis.table([("value", "string")], name="t")
    expr = t["value"].substr(3, -1)
    with pytest.raises(Exception) as exception_info:
        ibis_bigquery.compile(expr)

    expected = "Length parameter should not be a negative value."
    assert str(exception_info.value) == expected


def test_bucket():
    t = ibis.table([("value", "double")], name="t")
    buckets = [0, 1, 3]
    expr = t.value.bucket(buckets).name("foo")
    result = ibis_bigquery.compile(expr)
    expected_name = "tmp" if IBIS_VERSION < IBIS_3_0_VERSION else "foo"
    expected = f"""\
SELECT
  CASE
    WHEN (0 <= `value`) AND (`value` < 1) THEN 0
    WHEN (1 <= `value`) AND (`value` <= 3) THEN 1
    ELSE CAST(NULL AS INT64)
  END AS `{expected_name}`
FROM t"""
    expected_2 = f"""\
SELECT
  CASE
    WHEN (`value` >= 0) AND (`value` < 1) THEN 0
    WHEN (`value` >= 1) AND (`value` <= 3) THEN 1
    ELSE CAST(NULL AS INT64)
  END AS `{expected_name}`
FROM t"""
    assert result == expected or result == expected_2


@pytest.mark.parametrize(
    ("kind", "begin", "end", "expected"),
    [
        ("preceding", None, 1, "UNBOUNDED PRECEDING AND 1 PRECEDING"),
        ("following", 1, None, "1 FOLLOWING AND UNBOUNDED FOLLOWING"),
    ],
)
def test_window_unbounded(kind, begin, end, expected):
    t = ibis.table([("a", "int64")], name="t")
    kwargs = {kind: (begin, end)}
    expr = t.a.sum().over(ibis.window(**kwargs))
    result = ibis_bigquery.compile(expr)
    expected_name = "tmp" if IBIS_VERSION < IBIS_3_0_VERSION else "sum"
    assert (
        result
        == f"""\
SELECT sum(`a`) OVER (ROWS BETWEEN {expected}) AS `{expected_name}`
FROM t"""
    )


def test_large_compile():
    """
    Tests that compiling a large expression tree finishes
    within a reasonable amount of time
    """
    num_columns = 20
    num_joins = 7

    class MockBackend(ibis_bigquery.Backend):
        pass

    names = [f"col_{i}" for i in range(num_columns)]
    schema = ibis.Schema(names, ["string"] * num_columns)
    ibis_client = MockBackend()
    table = TableExpr(ops.SQLQueryResult("select * from t", schema, ibis_client))
    for _ in range(num_joins):
        table = table.mutate(dummy=ibis.literal(""))
        table = table.left_join(table, ["dummy"])[[table]]

    start = datetime.datetime.now()
    table.compile()
    delta = datetime.datetime.now() - start
    assert delta.total_seconds() < 10


@pytest.mark.parametrize(
    ("operation", "sql", "keywords"),
    [
        ("union", "UNION ALL", {"distinct": False}),
        ("union", "UNION DISTINCT", {"distinct": True}),
        ("intersect", "INTERSECT DISTINCT", {}),
        ("difference", "EXCEPT DISTINCT", {}),
    ],
)
def test_set_operation(operation, sql, keywords):
    t0 = ibis.table([("a", "int64")], name="t0")
    t1 = ibis.table([("a", "int64")], name="t1")
    expr = getattr(t0, operation)(t1, **keywords)
    result = ibis_bigquery.compile(expr)

    query = f"""\
SELECT *
FROM t0
{sql}
SELECT *
FROM t1"""

    assert result == query


def test_geospatial_point():
    t = ibis.table([("lon", "float64"), ("lat", "float64")], name="t")
    expr = ibis.geo_point(t.lon, t.lat)
    result = ibis_bigquery.compile(expr)
    query = """\
SELECT ST_GEOGPOINT(`lon`, `lat`) AS `tmp`
FROM t"""
    assert result == query


def test_geospatial_azimuth():
    t = ibis.table([("p0", "point"), ("p1", "point")], name="t")
    expr = t.p0.azimuth(t.p1)
    result = ibis_bigquery.compile(expr)
    query = """\
SELECT ST_AZIMUTH(`p0`, `p1`) AS `tmp`
FROM t"""
    assert result == query


def test_geospatial_unary_union():
    t = ibis.table([("geog", "geography")], name="t")
    expr = t.geog.unary_union()
    result = ibis_bigquery.compile(expr)
    query = """\
SELECT ST_UNION_AGG(`geog`) AS `union`
FROM t"""
    assert result == query


@pytest.mark.parametrize(
    ("operation", "keywords", "function", "function_args"),
    [
        ("area", {}, "ST_AREA", []),
        ("as_binary", {}, "ST_ASBINARY", []),
        ("as_text", {}, "ST_ASTEXT", ()),
        ("buffer", {"radius": 5.2}, "ST_BUFFER", ["5.2"]),
        ("centroid", {}, "ST_CENTROID", []),
        ("end_point", {}, "ST_ENDPOINT", []),
        ("geometry_type", {}, "ST_GEOMETRYTYPE", []),
        ("length", {}, "ST_LENGTH", []),
        ("n_points", {}, "ST_NUMPOINTS", []),
        ("perimeter", {}, "ST_PERIMETER", []),
        ("point_n", {"n": 3}, "ST_POINTN", ["3"]),
        ("start_point", {}, "ST_STARTPOINT", []),
    ],
)
def test_geospatial_unary(operation, keywords, function, function_args):
    t = ibis.table([("geog", "geography")], name="t")
    expr = getattr(t.geog, operation)(**keywords)
    result = ibis_bigquery.compile(expr)
    fn_args = ", ".join([""] + function_args) if function_args else ""
    query = f"""\
SELECT {function}(`geog`{fn_args}) AS `tmp`
FROM t"""
    assert result == query


@pytest.mark.parametrize(
    ("operation", "keywords", "function", "function_args"),
    [
        ("contains", {}, "ST_CONTAINS", []),
        ("covers", {}, "ST_COVERS", []),
        ("covered_by", {}, "ST_COVEREDBY", []),
        ("d_within", {"distance": 5.2}, "ST_DWITHIN", ["5.2"]),
        ("difference", {}, "ST_DIFFERENCE", []),
        ("disjoint", {}, "ST_DISJOINT", []),
        ("distance", {}, "ST_DISTANCE", []),
        ("geo_equals", {}, "ST_EQUALS", []),
        ("intersection", {}, "ST_INTERSECTION", []),
        ("intersects", {}, "ST_INTERSECTS", []),
        ("max_distance", {}, "ST_MAXDISTANCE", []),
        ("touches", {}, "ST_TOUCHES", []),
        ("union", {}, "ST_UNION", []),
        ("within", {}, "ST_WITHIN", []),
    ],
)
def test_geospatial_binary(operation, keywords, function, function_args):
    t = ibis.table([("geog0", "geography"), ("geog1", "geography")], name="t")
    expr = getattr(t.geog0, operation)(t.geog1, **keywords)
    result = ibis_bigquery.compile(expr)
    fn_args = ", ".join([""] + function_args) if function_args else ""
    query = f"""\
SELECT {function}(`geog0`, `geog1`{fn_args}) AS `tmp`
FROM t"""
    assert result == query


@pytest.mark.parametrize(
    ("operation", "dimension_name"),
    [
        ("x_max", "xmax"),
        ("x_min", "xmin"),
        ("y_max", "ymax"),
        ("y_min", "ymin"),
    ],
)
def test_geospatial_minmax(operation, dimension_name):
    t = ibis.table([("geog", "geography")], name="t")
    expr = getattr(t.geog, operation)()
    result = ibis_bigquery.compile(expr)
    query = f"""\
SELECT ST_BOUNDINGBOX(`geog`).{dimension_name} AS `tmp`
FROM t"""
    assert result == query


@pytest.mark.parametrize(
    "dimension_name",
    [
        "x",
        "y",
    ],
)
def test_geospatial_xy(dimension_name):
    t = ibis.table([("pt", "point")], name="t")
    expr = getattr(t.pt, dimension_name)()
    result = ibis_bigquery.compile(expr)
    query = f"""\
SELECT ST_{dimension_name.upper()}(`pt`) AS `tmp`
FROM t"""
    assert result == query


def test_geospatial_simplify():
    t = ibis.table([("geog", "geography")], name="t")

    expr = t.geog.simplify(5.2, preserve_collapsed=False)
    result = ibis_bigquery.compile(expr)
    query = """\
SELECT ST_SIMPLIFY(`geog`, 5.2) AS `tmp`
FROM t"""
    assert result == query

    expr = t.geog.simplify(5.2, preserve_collapsed=True)
    with pytest.raises(Exception) as exception_info:
        ibis_bigquery.compile(expr)
    expected = "BigQuery simplify does not support preserving collapsed geometries, must pass preserve_collapsed=False"
    assert str(exception_info.value) == expected
