"""Backports for changes in Ibis."""

from ibis.backends.base.sql import compiler as sql_compiler
from ibis.backends.base.sql.compiler import query_builder


try:
    Difference = sql_compiler.Difference
except AttributeError:
    Difference = query_builder.Difference

try:
    Intersection = sql_compiler.Intersection
except AttributeError:
    Intersection = query_builder.Intersection

__all__ = ["Difference", "Intersection"]
