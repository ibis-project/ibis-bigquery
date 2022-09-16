import re

import ibis
import ibis.expr.datatypes as dt
import packaging.version
import pytest
from pytest import param

pytestmark = pytest.mark.bigquery

IBIS_VERSION = packaging.version.Version(ibis.__version__)
IBIS_1_VERSION = packaging.version.Version("1.4.0")
IBIS_3_0_VERSION = packaging.version.Version("3.0.0")
IBIS_3_2_VERSION = packaging.version.Version("3.2.0")

older_than_3 = pytest.mark.xfail(
    IBIS_VERSION < IBIS_3_0_VERSION, reason="requires ibis >= 3"
)
at_least_3 = pytest.mark.xfail(
    IBIS_VERSION >= IBIS_3_0_VERSION, reason="requires ibis < 3"
)
older_than_32 = pytest.mark.xfail(
    IBIS_VERSION < IBIS_3_2_VERSION, reason="requires ibis >= 3.2"
)
at_least_32 = pytest.mark.xfail(
    IBIS_VERSION >= IBIS_3_2_VERSION, reason="requires ibis < 3.2"
)


@pytest.mark.parametrize(
    "pattern",
    [
        param(r"@param_\d+", marks=[older_than_3], id="ibis3"),
        param("@param", marks=[at_least_3], id="not_ibis3"),
    ],
)
def test_timestamp_accepts_date_literals(alltypes, project_id, dataset_id, pattern):
    date_string = "2009-03-01"
    param = ibis.param(dt.timestamp).name("param_0")
    expr = alltypes.mutate(param=param)
    params = {param: date_string}
    result = expr.compile(params=params)
    expected = f"""\
SELECT \\*, {pattern} AS `param`
FROM `{project_id}\\.{dataset_id}\\.functional_alltypes`"""
    assert re.match(expected, result) is not None


@pytest.mark.parametrize(
    ("distinct", "expected_keyword"), [(True, "DISTINCT"), (False, "ALL")]
)
def test_union(alltypes, distinct, expected_keyword, project_id, dataset_id):
    expr = alltypes.union(alltypes, distinct=distinct)
    result = expr.compile()
    expected = f"""\
SELECT *
FROM `{project_id}.{dataset_id}.functional_alltypes`
UNION {expected_keyword}
SELECT *
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


def test_ieee_divide(alltypes, project_id, dataset_id):
    expr = alltypes.double_col / 0
    result = expr.compile()
    expected = f"""\
SELECT IEEE_DIVIDE(`double_col`, 0) AS `tmp`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


def test_floor_divide(alltypes, project_id, dataset_id):
    expr = alltypes.double_col // 0
    result = expr.compile()
    expected = f"""\
SELECT CAST(FLOOR(IEEE_DIVIDE(`double_col`, 0)) AS INT64) AS `tmp`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


def test_identical_to(alltypes, project_id, dataset_id):
    t = alltypes
    pred = t.string_col.identical_to("a") & t.date_string_col.identical_to("b")
    expr = t[pred]
    result = expr.compile()
    expected = f"""\
SELECT *
FROM `{project_id}.{dataset_id}.functional_alltypes`
WHERE (((`string_col` IS NULL) AND ('a' IS NULL)) OR (`string_col` = 'a')) AND (((`date_string_col` IS NULL) AND ('b' IS NULL)) OR (`date_string_col` = 'b'))"""  # noqa: E501
    expected_2 = f"""\
SELECT *
FROM `{project_id}.{dataset_id}.functional_alltypes`
WHERE (((`string_col` IS NULL) AND ('a' IS NULL)) OR (`string_col` = 'a')) AND
      (((`date_string_col` IS NULL) AND ('b' IS NULL)) OR (`date_string_col` = 'b'))"""  # noqa: E501
    assert result == expected or result == expected_2


@pytest.mark.parametrize("timezone", [None, "America/New_York"])
def test_to_timestamp(alltypes, timezone, project_id, dataset_id):
    expr = alltypes.date_string_col.to_timestamp("%F", timezone)
    result = expr.compile()
    if timezone:
        expected = f"""\
SELECT PARSE_TIMESTAMP('%F', `date_string_col`, 'America/New_York') AS `tmp`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    else:
        expected = f"""\
SELECT PARSE_TIMESTAMP('%F', `date_string_col`) AS `tmp`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


def test_window_function(alltypes, project_id, dataset_id):
    t = alltypes
    w1 = ibis.window(
        preceding=1, following=0, group_by="year", order_by="timestamp_col"
    )
    expr = t.mutate(win_avg=t.float_col.mean().over(w1))
    result = expr.compile()
    expected = f"""\
SELECT *,
       avg(`float_col`) OVER (PARTITION BY `year` ORDER BY `timestamp_col` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS `win_avg`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected

    w2 = ibis.window(
        preceding=0, following=2, group_by="year", order_by="timestamp_col"
    )
    expr = t.mutate(win_avg=t.float_col.mean().over(w2))
    result = expr.compile()
    expected = f"""\
SELECT *,
       avg(`float_col`) OVER (PARTITION BY `year` ORDER BY `timestamp_col` ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS `win_avg`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected

    w3 = ibis.window(preceding=(4, 2), group_by="year", order_by="timestamp_col")
    expr = t.mutate(win_avg=t.float_col.mean().over(w3))
    result = expr.compile()
    expected = f"""\
SELECT *,
       avg(`float_col`) OVER (PARTITION BY `year` ORDER BY `timestamp_col` ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) AS `win_avg`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_range_window_function(alltypes, project_id, dataset_id):
    t = alltypes
    w = ibis.range_window(preceding=1, following=0, group_by="year", order_by="month")
    expr = t.mutate(two_month_avg=t.float_col.mean().over(w))
    result = expr.compile()
    expected = f"""\
SELECT *,
       avg(`float_col`) OVER (PARTITION BY `year` ORDER BY `month` RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS `two_month_avg`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected

    w3 = ibis.range_window(preceding=(4, 2), group_by="year", order_by="timestamp_col")
    expr = t.mutate(win_avg=t.float_col.mean().over(w3))
    result = expr.compile()
    expected = f"""\
SELECT *,
       avg(`float_col`) OVER (PARTITION BY `year` ORDER BY UNIX_MICROS(`timestamp_col`) RANGE BETWEEN 4 PRECEDING AND 2 PRECEDING) AS `win_avg`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.parametrize(
    ("preceding", "value"),
    [
        (5, 5),
        (ibis.interval(nanoseconds=1), 0.001),
        (ibis.interval(microseconds=1), 1),
        (ibis.interval(seconds=1), 1000000),
        (ibis.interval(minutes=1), 1000000 * 60),
        (ibis.interval(hours=1), 1000000 * 60 * 60),
        (ibis.interval(days=1), 1000000 * 60 * 60 * 24),
        (2 * ibis.interval(days=1), 1000000 * 60 * 60 * 24 * 2),
        (ibis.interval(weeks=1), 1000000 * 60 * 60 * 24 * 7),
    ],
)
@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_trailing_range_window(alltypes, preceding, value, project_id, dataset_id):
    t = alltypes
    w = ibis.trailing_range_window(preceding=preceding, order_by=t.timestamp_col)
    expr = t.mutate(win_avg=t.float_col.mean().over(w))
    result = expr.compile()
    expected = f"""\
SELECT *,
       avg(`float_col`) OVER (ORDER BY UNIX_MICROS(`timestamp_col`) RANGE BETWEEN {value} PRECEDING AND CURRENT ROW) AS `win_avg`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.parametrize("preceding", [ibis.interval(years=1)])
@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_trailing_range_window_unsupported(alltypes, preceding):
    t = alltypes
    w = ibis.trailing_range_window(preceding=preceding, order_by=t.timestamp_col)
    expr = t.mutate(win_avg=t.float_col.mean().over(w))
    with pytest.raises(ValueError):
        expr.compile()


@pytest.mark.parametrize(
    ("distinct1", "distinct2", "expected1", "expected2"),
    [
        (True, True, "UNION DISTINCT", "UNION DISTINCT"),
        (True, False, "UNION DISTINCT", "UNION ALL"),
        (False, True, "UNION ALL", "UNION DISTINCT"),
        (False, False, "UNION ALL", "UNION ALL"),
    ],
)
def test_union_cte(
    alltypes, distinct1, distinct2, expected1, expected2, project_id, dataset_id
):
    t = alltypes
    expr1 = t.group_by(t.string_col).aggregate(metric=t.double_col.sum())
    expr2 = expr1.view()
    expr3 = expr1.view()
    expr = expr1.union(expr2, distinct=distinct1).union(expr3, distinct=distinct2)
    result = expr.compile()
    expected = f"""\
WITH t0 AS (
  SELECT `string_col`, sum(`double_col`) AS `metric`
  FROM `{project_id}.{dataset_id}.functional_alltypes`
  GROUP BY 1
)
SELECT *
FROM t0
{expected1}
SELECT `string_col`, sum(`double_col`) AS `metric`
FROM `{project_id}.{dataset_id}.functional_alltypes`
GROUP BY 1
{expected2}
SELECT `string_col`, sum(`double_col`) AS `metric`
FROM `{project_id}.{dataset_id}.functional_alltypes`
GROUP BY 1"""
    assert result == expected


def test_bool_reducers(alltypes, project_id, dataset_id):
    b = alltypes.bool_col
    expr = b.mean()
    result = expr.compile()
    expected = f"""\
SELECT avg(CAST(`bool_col` AS INT64)) AS `mean`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected

    expr2 = b.sum()
    result = expr2.compile()
    expected = f"""\
SELECT sum(CAST(`bool_col` AS INT64)) AS `sum`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN `month` > 6 THEN CAST(`bool_col` AS INT64) ELSE NULL END",
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if(`month` > 6, CAST(`bool_col` AS INT64), NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
def test_bool_reducers_where_simple(alltypes, expected, project_id, dataset_id):
    b = alltypes.bool_col
    m = alltypes.month
    expr = b.mean(where=m > 6)
    result = expr.compile()
    expected = f"""\
SELECT avg({expected}) AS `mean`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN (`month` > 6) AND (`month` < 10) THEN CAST(`bool_col` AS INT64) ELSE NULL END",  # noqa: E501
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if((`month` > 6) AND (`month` < 10), CAST(`bool_col` AS INT64), NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
def test_bool_reducers_where_conj(alltypes, expected, project_id, dataset_id):
    b = alltypes.bool_col
    m = alltypes.month
    expr2 = b.sum(where=((m > 6) & (m < 10)))
    result = expr2.compile()
    expected = f"""\
SELECT sum({expected}) AS `sum`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


def test_approx_nunique(alltypes, project_id, dataset_id):
    d = alltypes.double_col
    expr = d.approx_nunique()
    result = expr.compile()
    expected = f"""\
SELECT APPROX_COUNT_DISTINCT(`double_col`) AS `approx_nunique`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN `month` > 6 THEN `bool_col` ELSE NULL END",
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if(`month` > 6, `bool_col`, NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
def test_approx_nunique_cond(alltypes, expected, project_id, dataset_id):
    b = alltypes.bool_col
    m = alltypes.month
    expr2 = b.approx_nunique(where=m > 6)
    result = expr2.compile()
    expected = f"""\
SELECT APPROX_COUNT_DISTINCT({expected}) AS `approx_nunique`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


def test_approx_median(alltypes, project_id, dataset_id):
    d = alltypes.double_col
    expr = d.approx_median()
    result = expr.compile()
    expected = f"""\
SELECT APPROX_QUANTILES(`double_col`, 2)[OFFSET(1)] AS `approx_median`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN `month` > 6 THEN `double_col` ELSE NULL END",
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if(`month` > 6, `double_col`, NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
def test_approx_median_cond(alltypes, expected, project_id, dataset_id):
    m = alltypes.month
    d = alltypes.double_col
    expr2 = d.approx_median(where=m > 6)
    result = expr2.compile()
    expected = f"""\
SELECT APPROX_QUANTILES({expected}, 2)[OFFSET(1)] AS `approx_median`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_bit_and(alltypes, project_id, dataset_id):
    i = alltypes.int_col
    expr = i.bit_and()
    result = expr.compile()
    expected = f"""\
SELECT BIT_AND(`int_col`) AS `bit_and`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN `bigint_col` > 6 THEN `int_col` ELSE NULL END",
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if(`bigint_col` > 6, `int_col`, NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_bit_and_cond(alltypes, expected, project_id, dataset_id):
    b = alltypes.bigint_col
    i = alltypes.int_col
    expr2 = i.bit_and(where=b > 6)
    result = expr2.compile()
    expected = f"""\
SELECT BIT_AND({expected}) AS `bit_and`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_bit_or(alltypes, project_id, dataset_id):
    i = alltypes.int_col
    expr = i.bit_or()
    result = expr.compile()
    expected = f"""\
SELECT BIT_OR(`int_col`) AS `bit_or`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN `bigint_col` > 6 THEN `int_col` ELSE NULL END",
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if(`bigint_col` > 6, `int_col`, NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_bit_or_cond(alltypes, expected, project_id, dataset_id):
    b = alltypes.bigint_col
    i = alltypes.int_col
    expr2 = i.bit_or(where=b > 6)
    result = expr2.compile()
    expected = f"""\
SELECT BIT_OR({expected}) AS `bit_or`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_bit_xor(alltypes, project_id, dataset_id):
    i = alltypes.int_col
    expr = i.bit_xor()
    result = expr.compile()
    expected = f"""\
SELECT BIT_XOR(`int_col`) AS `bit_xor`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected


@pytest.mark.parametrize(
    "expected",
    [
        param(
            "CASE WHEN `bigint_col` > 6 THEN `int_col` ELSE NULL END",
            id="ibis_not32",
            marks=at_least_32,
        ),
        param(
            "if(`bigint_col` > 6, `int_col`, NULL)",
            id="ibis_32",
            marks=older_than_32,
        ),
    ],
)
@pytest.mark.skipif(IBIS_VERSION <= IBIS_1_VERSION, reason="requires ibis 2.x")
def test_bit_xor_cond(alltypes, expected, project_id, dataset_id):
    b = alltypes.bigint_col
    i = alltypes.int_col
    expr2 = i.bit_xor(where=b > 6)
    result = expr2.compile()
    expected = f"""\
SELECT BIT_XOR({expected}) AS `bit_xor`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""  # noqa: E501
    assert result == expected


def test_cov(alltypes, project_id, dataset_id):
    d = alltypes.double_col
    expr = d.cov(d)
    result = expr.compile()
    expected = f"""\
SELECT COVAR_SAMP(`double_col`, `double_col`) AS `tmp`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected

    expr = d.cov(d, how="pop")
    result = expr.compile()
    expected = f"""\
SELECT COVAR_POP(`double_col`, `double_col`) AS `tmp`
FROM `{project_id}.{dataset_id}.functional_alltypes`"""
    assert result == expected

    with pytest.raises(ValueError):
        d.cov(d, how="error")
