import collections
import datetime
import decimal
import re

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import numpy as np
import packaging.version
import pandas as pd
import pandas.testing as tm
import pytest
import pytz
from pytest import param

import ibis_bigquery
from ibis_bigquery.client import bigquery_param

IBIS_VERSION = packaging.version.Version(ibis.__version__)
IBIS_1_4_VERSION = packaging.version.Version("1.4.0")
IBIS_3_0_VERSION = packaging.version.Version("3.0.0")
IBIS_3_2_VERSION = packaging.version.Version("3.2.0")

xfail_older_than_3 = pytest.mark.xfail(
    IBIS_VERSION < IBIS_3_0_VERSION, reason="requires ibis >= 3"
)
xfail_older_than_3_and_not_32 = pytest.mark.xfail(
    not (IBIS_3_0_VERSION <= IBIS_VERSION < IBIS_3_2_VERSION),
    reason="requires 3.0 < ibis < 3.2",
)
xfail_at_least_3 = pytest.mark.xfail(
    IBIS_VERSION >= IBIS_3_0_VERSION, reason="requires ibis < 3"
)
xfail_older_than_32 = pytest.mark.xfail(
    IBIS_VERSION < IBIS_3_2_VERSION, reason="requires ibis >= 3.2"
)


def test_table(alltypes):
    assert isinstance(alltypes, ir.TableExpr)


def test_column_execute(alltypes, df):
    col_name = "float_col"
    expr = alltypes[col_name]
    result = expr.execute()
    expected = df[col_name]
    tm.assert_series_equal(
        # Sort the values because BigQuery doesn't guarantee row order unless
        # there is an order-by clause in the query.
        result.sort_values().reset_index(drop=True),
        expected.sort_values().reset_index(drop=True),
    )


def test_literal_execute(client):
    expected = "1234"
    expr = ibis.literal(expected)
    result = client.execute(expr)
    assert result == expected


def test_simple_aggregate_execute(alltypes, df):
    col_name = "float_col"
    expr = alltypes[col_name].sum()
    result = expr.execute()
    expected = df[col_name].sum()
    np.testing.assert_allclose(result, expected)


def test_list_tables(client):
    tables = client.list_tables(like="functional_alltypes")
    assert set(tables) == {"functional_alltypes", "functional_alltypes_parted"}


def test_current_database(client, dataset_id):
    assert client.current_database == dataset_id
    assert client.current_database == client.dataset_id
    assert client.list_tables(database=client.current_database) == client.list_tables()


def test_database(client):
    database = client.database(client.dataset_id)
    assert database.list_tables() == client.list_tables()


def test_compile_toplevel():
    t = ibis.table([("foo", "double")], name="t0")

    # it works!
    expr = t.foo.sum()
    result = ibis_bigquery.compile(expr)
    # FIXME: remove quotes because bigquery can't use anythig that needs
    # quoting?
    expected = """\
SELECT sum(`foo`) AS `sum`
FROM t0"""  # noqa
    assert str(result) == expected


def test_struct_field_access(struct_table):
    expr = struct_table.struct_col["string_field"]
    result = expr.execute()
    expected_name = "tmp" if IBIS_VERSION < IBIS_3_0_VERSION else "string_field"
    expected = pd.Series([None, "a"], name=expected_name)
    tm.assert_series_equal(result, expected)


def test_array_index(struct_table):
    expr = struct_table.array_of_structs_col[1]
    result = expr.execute()
    expected = pd.Series(
        [
            {"int_field": None, "string_field": None},
            {"int_field": None, "string_field": "hijklmnop"},
        ],
        name="tmp",
    )
    tm.assert_series_equal(result, expected)


def test_array_concat(struct_table):
    c = struct_table.array_of_structs_col
    expr = c + c
    result = expr.execute()
    expected = pd.Series(
        [
            [
                {"int_field": 12345, "string_field": "abcdefg"},
                {"int_field": None, "string_field": None},
                {"int_field": 12345, "string_field": "abcdefg"},
                {"int_field": None, "string_field": None},
            ],
            [
                {"int_field": 12345, "string_field": "abcdefg"},
                {"int_field": None, "string_field": "hijklmnop"},
                {"int_field": 12345, "string_field": "abcdefg"},
                {"int_field": None, "string_field": "hijklmnop"},
            ],
        ],
        name="tmp",
    )
    tm.assert_series_equal(result, expected)


def test_array_length(struct_table):
    expr = struct_table.array_of_structs_col.length()
    result = expr.execute()
    expected = pd.Series([2, 2], name="tmp")
    tm.assert_series_equal(result, expected)


def test_array_collect(struct_table):
    key = struct_table.array_of_structs_col[0]["string_field"]
    expr = struct_table.groupby(key=key).aggregate(
        foo=lambda t: t.array_of_structs_col[0]["int_field"].collect()
    )
    result = expr.execute()
    expected = struct_table.execute()
    expected = (
        expected.assign(
            key=expected.array_of_structs_col.apply(lambda x: x[0]["string_field"])
        )
        .groupby("key")
        .apply(
            lambda df: list(df.array_of_structs_col.apply(lambda x: x[0]["int_field"]))
        )
        .reset_index()
        .rename(columns={0: "foo"})
    )
    tm.assert_frame_equal(result, expected)


def test_count_distinct_with_filter(alltypes):
    expr = alltypes.string_col.nunique(where=alltypes.string_col.cast("int64") > 1)
    result = expr.execute()
    expected = alltypes.string_col.execute()
    expected = expected[expected.astype("int64") > 1].nunique()
    assert result == expected


@pytest.mark.parametrize("type", ["date", dt.date])
def test_cast_string_to_date(alltypes, df, type):
    import toolz

    string_col = alltypes.date_string_col
    month, day, year = toolz.take(3, string_col.split("/"))

    expr = "20" + ibis.literal("-").join([year, month, day])
    expr = expr.cast(type)

    result = (
        expr.execute()
        .astype("datetime64[ns]")
        .sort_values()
        .reset_index(drop=True)
        .rename("date_string_col")
    )
    expected = (
        pd.to_datetime(df.date_string_col)
        .dt.normalize()
        .sort_values()
        .reset_index(drop=True)
    )
    tm.assert_series_equal(result, expected)


def test_has_partitions(alltypes, parted_alltypes, client):
    col = client.partition_column
    assert col not in alltypes.columns
    assert col in parted_alltypes.columns


def test_different_partition_col_name(monkeypatch, client):
    col = "FOO_BAR"
    monkeypatch.setattr(client, "partition_column", col)
    alltypes = client.table("functional_alltypes")
    parted_alltypes = client.table("functional_alltypes_parted")
    assert col not in alltypes.columns
    assert col in parted_alltypes.columns


def scalar_params_ibis3(project_id, dataset_id):
    return f"""\
SELECT count\\(`foo`\\) AS `count`
FROM \\(
  SELECT `string_col`, sum\\(`float_col`\\) AS `foo`
  FROM \\(
    SELECT `float_col`, `timestamp_col`, `int_col`, `string_col`
    FROM `{project_id}\\.{dataset_id}\\.functional_alltypes`
  \\) t1
  WHERE `timestamp_col` < @param_\\d+
  GROUP BY 1
\\) t0"""


def scalar_params_not_ibis3(project_id, dataset_id):
    return f"""\
SELECT count\\(`foo`\\) AS `count`
FROM \\(
  SELECT `string_col`, sum\\(`float_col`\\) AS `foo`
  FROM \\(
    SELECT `float_col`, `timestamp_col`, `int_col`, `string_col`
    FROM `{project_id}\\.{dataset_id}\\.functional_alltypes`
    WHERE `timestamp_col` < @my_param
  \\) t1
  GROUP BY 1
\\) t0"""


def scalar_params_ibis32(project_id, dataset_id):
    return f"""\
SELECT count\\(`foo`\\) AS `count`
FROM \\(
  SELECT `string_col`, sum\\(`float_col`\\) AS `foo`
  FROM \\(
    SELECT `float_col`, `timestamp_col`, `int_col`, `string_col`
    FROM `{project_id}\\.{dataset_id}\\.functional_alltypes`
    WHERE `timestamp_col` < @param_\\d+
  \\) t1
  GROUP BY 1
\\) t0"""


@pytest.mark.parametrize(
    "expected_fn",
    [
        param(scalar_params_ibis3, marks=[xfail_older_than_3_and_not_32], id="ibis3"),
        param(scalar_params_not_ibis3, marks=[xfail_at_least_3], id="not_ibis3"),
        param(scalar_params_ibis32, marks=[xfail_older_than_32], id="ibis32"),
    ],
)
def test_subquery_scalar_params(alltypes, project_id, dataset_id, expected_fn):
    expected = expected_fn(project_id, dataset_id)
    t = alltypes
    param = ibis.param("timestamp").name("my_param")
    expr = (
        t[["float_col", "timestamp_col", "int_col", "string_col"]][
            lambda t: t.timestamp_col < param
        ]
        .groupby("string_col")
        .aggregate(foo=lambda t: t.float_col.sum())
        .foo.count()
    )
    result = expr.compile(params={param: "20140101"})
    assert re.match(expected, result) is not None


def test_scalar_param_string(alltypes, df):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("string")
    expr = alltypes[alltypes.string_col == param]

    string_value = "0"
    result = (
        expr.execute(params={param: string_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.string_col == string_value].sort_values("id").reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_int64(alltypes, df):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("int64")
    expr = alltypes[alltypes.string_col.cast("int64") == param]

    int64_value = 0
    result = (
        expr.execute(params={param: int64_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.string_col.astype("int64") == int64_value]
        .sort_values("id")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_double(alltypes, df):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("double")
    expr = alltypes[alltypes.string_col.cast("int64").cast("double") == param]

    double_value = 0.0
    result = (
        expr.execute(params={param: double_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.string_col.astype("int64").astype("float64") == double_value]
        .sort_values("id")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_boolean(alltypes, df):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("boolean")
    expr = alltypes[(alltypes.string_col.cast("int64") == 0) == param]

    bool_value = True
    result = (
        expr.execute(params={param: bool_value})
        .sort_values("id")
        .reset_index(drop=True)
    )
    expected = (
        df.loc[df.string_col.astype("int64") == 0]
        .sort_values("id")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "timestamp_value",
    [
        "2009-01-20 01:02:03",
        datetime.date(2009, 1, 20),
        datetime.datetime(2009, 1, 20, 1, 2, 3),
    ],
)
def test_scalar_param_timestamp(alltypes, df, timestamp_value):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("timestamp")
    expr = alltypes[alltypes.timestamp_col <= param][["timestamp_col"]]

    result = (
        expr.execute(params={param: timestamp_value})
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    value = pd.Timestamp(timestamp_value)
    expected = (
        df.loc[df.timestamp_col <= value, ["timestamp_col"]]
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "date_value",
    ["2009-01-20", datetime.date(2009, 1, 20), datetime.datetime(2009, 1, 20)],
)
def test_scalar_param_date(alltypes, df, date_value):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("date")
    expr = alltypes[alltypes.timestamp_col.cast("date") <= param]

    result = (
        expr.execute(params={param: date_value})
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    value = pd.Timestamp(date_value)
    expected = (
        df.loc[df.timestamp_col.dt.normalize() <= value]
        .sort_values("timestamp_col")
        .reset_index(drop=True)
    )
    tm.assert_frame_equal(result, expected)


def test_scalar_param_array(alltypes, df):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("array<double>")
    expr = alltypes.sort_by("id").limit(1).double_col.collect() + param
    result = expr.execute(params={param: [1]})
    expected = [df.sort_values("id").double_col.iat[0]] + [1.0]
    assert all(
        float(result[index]) == expected[index] for index in range(len(expected))
    )


def test_scalar_param_struct(client):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    struct_type = dt.Struct.from_tuples([("x", dt.int64), ("y", dt.string)])
    param = ibis.param(struct_type)
    value = collections.OrderedDict([("x", 1), ("y", "foobar")])
    result = client.execute(param, {param: value})
    assert value == result


def test_scalar_param_nested(client):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    param = ibis.param("struct<x: array<struct<y: array<double>>>>")
    value = collections.OrderedDict(
        [("x", [collections.OrderedDict([("y", [1.0, 2.0, 3.0])])])]
    )
    result = client.execute(param, {param: value})
    assert all(
        float(res) == float(val)
        for res, val in zip(value["x"][0]["y"], result["x"][0]["y"])
    )


def test_repr_struct_of_array_of_struct():
    param = ibis.param("struct<x: array<struct<y: array<double>>>>")
    param = param.name("foo")
    value = collections.OrderedDict(
        [("x", [collections.OrderedDict([("y", [1.0, 2.0, 3.0])])])]
    )
    result = bigquery_param(param, value)
    expected = {
        "name": "foo",
        "parameterType": {
            "structTypes": [
                {
                    "name": "x",
                    "type": {
                        "arrayType": {
                            "structTypes": [
                                {
                                    "name": "y",
                                    "type": {
                                        "arrayType": {"type": "FLOAT64"},
                                        "type": "ARRAY",
                                    },
                                }
                            ],
                            "type": "STRUCT",
                        },
                        "type": "ARRAY",
                    },
                }
            ],
            "type": "STRUCT",
        },
        "parameterValue": {
            "structValues": {
                "x": {
                    "arrayValues": [
                        {
                            "structValues": {
                                "y": {
                                    "arrayValues": [
                                        {"value": 1.0},
                                        {"value": 2.0},
                                        {"value": 3.0},
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        },
    }
    assert result.to_api_repr() == expected


def test_raw_sql(client):
    assert client.raw_sql("SELECT 1").fetchall() == [(1,)]


@pytest.mark.parametrize(
    "pattern",
    [
        param(r"@param_\d+", marks=[xfail_older_than_3], id="ibis3"),
        param("@param", marks=[xfail_at_least_3], id="not_ibis3"),
    ],
)
def test_scalar_param_scope(alltypes, project_id, dataset_id, pattern):
    t = alltypes
    param = ibis.param("timestamp")
    result = t.mutate(param=param).compile(params={param: "2017-01-01"})
    expected = f"""\
SELECT \\*, {pattern} AS `param`
FROM `{project_id}\\.{dataset_id}\\.functional_alltypes`"""
    assert re.match(expected, result) is not None


def test_parted_column_rename(parted_alltypes):
    assert "PARTITIONTIME" in parted_alltypes.columns
    assert "_PARTITIONTIME" in parted_alltypes.op().table.columns


def test_scalar_param_partition_time(parted_alltypes):
    if IBIS_VERSION < IBIS_1_4_VERSION:
        pytest.skip("requires ibis 1.4+")
    assert "PARTITIONTIME" in parted_alltypes.columns
    assert "PARTITIONTIME" in parted_alltypes.schema()
    param = ibis.param("timestamp").name("time_param")
    expr = parted_alltypes[parted_alltypes.PARTITIONTIME < param]
    df = expr.execute(params={param: "2017-01-01"})
    assert df.empty


def test_exists_table(client):
    assert client.exists_table("functional_alltypes")
    assert not client.exists_table("footable")


def test_exists_database(client, dataset_id):
    assert client.exists_database(dataset_id)
    assert not client.exists_database("foodataset")


@pytest.mark.parametrize("kind", ["date", "timestamp"])
def test_parted_column(client, kind):
    table_name = "{}_column_parted".format(kind)
    t = client.table(table_name)
    expected_column = "my_{}_parted_col".format(kind)
    assert t.columns == [expected_column, "string_col", "int_col"]


def test_cross_project_query(public):
    table = public.table("posts_questions")
    expr = table[table.tags.contains("ibis")][["title", "tags"]]
    result = expr.compile()
    expected = """\
SELECT `title`, `tags`
FROM (
  SELECT *
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE STRPOS(`tags`, 'ibis') - 1 >= 0
) t0"""
    assert result == expected
    n = 5
    df = expr.limit(n).execute()
    assert len(df) == n
    assert list(df.columns) == ["title", "tags"]
    assert df.title.dtype == np.object
    assert df.tags.dtype == np.object


def test_set_database(client2):
    client2.set_database("bigquery-public-data.epa_historical_air_quality")
    tables = client2.list_tables()
    assert "co_daily_summary" in tables


def test_exists_table_different_project(client):
    name = "co_daily_summary"
    database = "bigquery-public-data.epa_historical_air_quality"
    assert client.exists_table(name, database=database)
    assert not client.exists_table("foobar", database=database)


def test_exists_table_different_project_fully_qualified(client):
    name = "bigquery-public-data.epa_historical_air_quality.co_daily_summary"
    assert client.exists_table(name)
    assert not client.exists_table(
        "bigquery-public-data.epa_historical_air_quality.foobar"
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("bigquery-public-data.epa_historical_air_quality", True),
        ("bigquery-foo-bar-project.baz_dataset", False),
    ],
)
def test_exists_database_different_project(client, name, expected):
    assert client.exists_database(name) is expected


def test_multiple_project_queries(client):
    so = client.table("posts_questions", database="bigquery-public-data.stackoverflow")
    trips = client.table("trips", database="nyc-tlc.yellow")
    join = so.join(trips, so.tags == trips.rate_code)[[so.title]]
    result = join.compile()
    expected = """\
SELECT t0.`title`
FROM `bigquery-public-data.stackoverflow.posts_questions` t0
  INNER JOIN `nyc-tlc.yellow.trips` t1
    ON t0.`tags` = t1.`rate_code`"""
    assert result == expected


def test_multiple_project_queries_database_api(client):
    stackoverflow = client.database("bigquery-public-data.stackoverflow")
    posts_questions = stackoverflow.posts_questions
    yellow = client.database("nyc-tlc.yellow")
    trips = yellow.trips
    predicate = posts_questions.tags == trips.rate_code
    join = posts_questions.join(trips, predicate)[[posts_questions.title]]
    result = join.compile()
    expected = """\
SELECT t0.`title`
FROM `bigquery-public-data.stackoverflow.posts_questions` t0
  INNER JOIN `nyc-tlc.yellow.trips` t1
    ON t0.`tags` = t1.`rate_code`"""
    assert result == expected


def test_multiple_project_queries_execute(client):
    stackoverflow = client.database("bigquery-public-data.stackoverflow")
    posts_questions = stackoverflow.posts_questions.limit(5)
    yellow = client.database("nyc-tlc.yellow")
    trips = yellow.trips.limit(5)
    predicate = posts_questions.tags == trips.rate_code
    cols = [posts_questions.title]
    join = posts_questions.left_join(trips, predicate)[cols]
    result = join.execute()
    assert list(result.columns) == ["title"]
    assert len(result) == 5


def test_large_timestamp(client):
    huge_timestamp = datetime.datetime(
        year=4567, month=1, day=1, tzinfo=datetime.timezone.utc
    )
    expr = ibis.timestamp("4567-01-01 00:00:00")
    result = client.execute(expr)
    assert result.astimezone(datetime.timezone.utc) == huge_timestamp


def test_string_to_timestamp(client):
    timestamp = pd.Timestamp(
        datetime.datetime(year=2017, month=2, day=6), tz=pytz.timezone("UTC")
    )
    expr = ibis.literal("2017-02-06").to_timestamp("%F")
    result = client.execute(expr)
    assert result == timestamp

    timestamp_tz = pd.Timestamp(
        datetime.datetime(year=2017, month=2, day=6, hour=5),
        tz=pytz.timezone("UTC"),
    )
    expr_tz = ibis.literal("2017-02-06").to_timestamp("%F", "America/New_York")
    result_tz = client.execute(expr_tz)
    assert result_tz == timestamp_tz


def test_client_sql_query(client, dataset_id):
    expr = client.sql(f"select * from {dataset_id}.functional_alltypes limit 20")
    result = expr.execute()
    expected = client.table("functional_alltypes").head(20).execute()
    tm.assert_frame_equal(result, expected)


def test_timestamp_column_parted_is_not_renamed(client):
    t = client.table("timestamp_column_parted")
    assert "_PARTITIONTIME" not in t.columns
    assert "PARTITIONTIME" not in t.columns


def test_prevent_rewrite(alltypes, project_id, dataset_id):
    t = alltypes
    expr = (
        t.groupby(t.string_col)
        .aggregate(collected_double=t.double_col.collect())
        .pipe(ibis.prevent_rewrite)
        .filter(lambda t: t.string_col != "wat")
    )
    result = expr.compile()
    expected = """\
SELECT *
FROM (
  SELECT `string_col`, ARRAY_AGG(`double_col`) AS `collected_double`
  FROM `{}.{}.functional_alltypes`
  GROUP BY 1
) t0
WHERE `string_col` != 'wat'""".format(
        project_id, dataset_id
    )
    assert result == expected


@pytest.mark.parametrize(
    ("case", "dtype"),
    [
        (datetime.date(2017, 1, 1), dt.date),
        (pd.Timestamp("2017-01-01"), dt.date),
        ("2017-01-01", dt.date),
        (datetime.datetime(2017, 1, 1, 4, 55, 59), dt.timestamp),
        ("2017-01-01 04:55:59", dt.timestamp),
        (pd.Timestamp("2017-01-01 04:55:59"), dt.timestamp),
    ],
)
def test_day_of_week(client, case, dtype):
    date_var = ibis.literal(case, type=dtype)
    expr_index = date_var.day_of_week.index()
    result = client.execute(expr_index)
    assert result == 6

    expr_name = date_var.day_of_week.full_name()
    result = client.execute(expr_name)
    assert result == "Sunday"


def test_boolean_reducers(alltypes):
    b = alltypes.bool_col
    bool_avg = b.mean().execute()
    assert type(bool_avg) == np.float64

    bool_sum = b.sum().execute()
    assert type(bool_sum) == np.int64


def test_column_summary(alltypes):
    bool_col_summary = alltypes.bool_col.summary()
    expr = alltypes.aggregate(bool_col_summary)
    result = expr.execute()
    assert result.shape == (1, 7)
    assert len(result) == 1


def test_numeric_table_schema(numeric_table):
    assert numeric_table.schema() == ibis.schema(
        [("string_col", dt.string), ("numeric_col", dt.Decimal(38, 9))]
    )


def test_numeric_sum(numeric_table):
    t = numeric_table
    expr = t.numeric_col.sum()
    result = expr.execute()
    assert isinstance(result, decimal.Decimal)
    compare = result.compare(decimal.Decimal("1.000000001"))
    assert compare == decimal.Decimal("0")


def test_boolean_casting(alltypes):
    t = alltypes
    expr = t.groupby(k=t.string_col.nullif("1") == "9").count()
    result = expr.execute().set_index("k")
    count = result["count"]
    assert count.at[False] == 5840
    assert count.at[True] == 730


def test_approx_median(alltypes):
    m = alltypes.month
    expected = m.execute().median()
    assert expected == 7

    expr = m.approx_median()
    result = expr.execute()
    # Since 6 and 7 are right on the edge for median in the range of months
    # (1-12), accept either for the approximate function.
    assert result in (6, 7)
