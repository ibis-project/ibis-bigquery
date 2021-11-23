import datetime

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import packaging.version
import pandas as pd
import pytest
from ibis.expr.types import TableExpr

import ibis_bigquery

IBIS_VERSION = packaging.version.Version(ibis.__version__)
IBIS_1_4_VERSION = packaging.version.Version("1.4.0")


@pytest.mark.parametrize(
    ("case", "expected", "dtype"),
    [
        (datetime.date(2017, 1, 1), "DATE '2017-01-01'", dt.date),
        (pd.Timestamp("2017-01-01"), "DATE '2017-01-01'", dt.date,),
        ("2017-01-01", "DATE '2017-01-01'", dt.date),
        (
            datetime.datetime(2017, 1, 1, 4, 55, 59),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        ("2017-01-01 04:55:59", "TIMESTAMP '2017-01-01 04:55:59'", dt.timestamp,),
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
    assert result == f"SELECT EXTRACT(year from {expected}) AS `tmp`"


@pytest.mark.parametrize(
    ("case", "expected", "dtype", "strftime_func"),
    [
        (datetime.date(2017, 1, 1), "DATE '2017-01-01'", dt.date, "FORMAT_DATE",),
        (pd.Timestamp("2017-01-01"), "DATE '2017-01-01'", dt.date, "FORMAT_DATE",),
        ("2017-01-01", "DATE '2017-01-01'", dt.date, "FORMAT_DATE",),
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
        ("test of hash", "'test of hash'", dt.string,),
        (b"test of hash", "FROM_BASE64('dGVzdCBvZiBoYXNo')", dt.binary,),
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
        ("test", "md5('test')", "md5", dt.string,),
        (b"test", "md5(FROM_BASE64('dGVzdA=='))", "md5", dt.binary,),
        ("test", "sha1('test')", "sha1", dt.string,),
        (b"test", "sha1(FROM_BASE64('dGVzdA=='))", "sha1", dt.binary,),
        ("test", "sha256('test')", "sha256", dt.string,),
        (b"test", "sha256(FROM_BASE64('dGVzdA=='))", "sha256", dt.binary,),
        ("test", "sha512('test')", "sha512", dt.string,),
        (b"test", "sha512(FROM_BASE64('dGVzdA=='))", "sha512", dt.binary,),
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
    ("case", "expected", "dtype"),
    [
        (
            datetime.datetime(2017, 1, 1, 4, 55, 59),
            "TIMESTAMP '2017-01-01 04:55:59'",
            dt.timestamp,
        ),
        ("2017-01-01 04:55:59", "TIMESTAMP '2017-01-01 04:55:59'", dt.timestamp,),
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
    assert result == f"SELECT EXTRACT(hour from {expected}) AS `tmp`"


def test_projection_fusion_only_peeks_at_immediate_parent():
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
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
    expected = """\
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
  INNER JOIN t3 t4"""
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
    expected = """\
SELECT CAST(`value` AS BYTES) AS `tmp`
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
    expected = """\
SELECT
  CASE
    WHEN (`value` >= 0) AND (`value` < 1) THEN 0
    WHEN (`value` >= 1) AND (`value` <= 3) THEN 1
    ELSE CAST(NULL AS INT64)
  END AS `tmp`
FROM t"""
    assert result == expected


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
    assert (
        result
        == f"""\
SELECT sum(`a`) OVER (ROWS BETWEEN {expected}) AS `tmp`
FROM t"""
    )


def test_large_compile():
    """
    Tests that compiling a large expression tree finishes
    within a reasonable amount of time
    """
    num_columns = 20
    num_joins = 7

    class MockBigQueryClient(ibis_bigquery.BigQueryClient):
        def __init__(self):
            pass

    names = [f"col_{i}" for i in range(num_columns)]
    schema = ibis.Schema(names, ["string"] * num_columns)
    ibis_client = MockBigQueryClient()
    table = TableExpr(ops.SQLQueryResult("select * from t", schema, ibis_client))
    for _ in range(num_joins):
        table = table.mutate(dummy=ibis.literal(""))
        table = table.left_join(table, ["dummy"])[[table]]

    start = datetime.datetime.now()
    table.compile()
    delta = datetime.datetime.now() - start
    assert delta.total_seconds() < 10
