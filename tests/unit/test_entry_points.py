"""Test to verify that the package is discoverable from Ibis v2."""

import ibis


def test_has_bigquery():
    assert hasattr(ibis, "bigquery")


def test_compile():
    expr = ibis.literal(1)
    result = ibis.bigquery.compile(expr)
    expected = "SELECT 1"
    assert expected in result
