"""Backports for changes in Ibis."""

import ibis
import packaging.version
from ibis.backends.base.sql.compiler import query_builder
from ibis.backends.base.sql.compiler.base import QueryAST, SetOp
from ibis.expr import operations as ops

IBIS_VERSION = packaging.version.Version(ibis.__version__)
IBIS_3_0_2_VERSION = packaging.version.Version("3.0.2")


def import_default(module_name, force=False, default=None):
    """
    Provide an implementation for a class or function when it can't be imported
    or when force is True.

    This is used to replicate Ibis APIs that are missing or insufficient
    (thus the force option) in early Ibis versions.

    Pulled from
    https://github.com/googleapis/python-db-dtypes-pandas/blob/1d32339fc72666d671d8d2dbca32e39abb3de94f/db_dtypes/pandas_backports.py#L50:5
    """

    if default is None:
        return lambda func_or_class: import_default(module_name, force, func_or_class)

    if force:
        return default

    name = default.__name__
    try:
        module = __import__(module_name, {}, {}, [name])
    except ModuleNotFoundError:
        return default

    return getattr(module, name, default)


@import_default("ibis.backends.base.sql.compiler", IBIS_VERSION <= IBIS_3_0_2_VERSION)
class Difference(SetOp):
    _keyword = "EXCEPT"

    def _get_keyword_list(self):
        return [self._keyword] * (len(self.tables) - 1)


@import_default("ibis.backends.base.sql.compiler", IBIS_VERSION <= IBIS_3_0_2_VERSION)
class Intersection(SetOp):
    _keyword = "INTERSECT"

    def _get_keyword_list(self):
        return [self._keyword] * (len(self.tables) - 1)


# Override Compiler to use overrides for Difference and Intersections. See
# https://github.com/ibis-project/ibis-bigquery/issues/87 and
# https://github.com/ibis-project/ibis/issues/3863
if IBIS_VERSION <= IBIS_3_0_2_VERSION:

    @classmethod  # type: ignore
    def _make_difference(cls, expr, context):
        # flatten differences so that we can codegen them all at once
        table_exprs = list(query_builder.flatten(expr))
        return cls.difference_class(table_exprs, expr, context=context)

    @classmethod  # type: ignore
    def _make_intersect(cls, expr, context):
        # flatten intersections so that we can codegen them all at once
        table_exprs = list(query_builder.flatten(expr))
        return cls.intersect_class(table_exprs, expr, context=context)

    @classmethod  # type: ignore
    def _make_union(cls, expr, context):
        # flatten unions so that we can codegen them all at once
        union_info = list(query_builder.flatten_union(expr))

        # since op is a union, we have at least 3 elements in union_info (left
        # distinct right) and if there is more than a single union we have an
        # additional two elements per union (distinct right) which means the
        # total number of elements is at least 3 + (2 * number of unions - 1)
        # and is therefore an odd number
        npieces = len(union_info)
        assert npieces >= 3 and npieces % 2 != 0, "Invalid union expression"

        # 1. every other object starting from 0 is a Table instance
        # 2. every other object starting from 1 is a bool indicating the type
        #    of union (distinct or not distinct)
        table_exprs, distincts = union_info[::2], union_info[1::2]
        return cls.union_class(table_exprs, expr, distincts=distincts, context=context)

    @classmethod  # type: ignore
    def to_ast(cls, expr, context=None):
        if context is None:
            context = cls.make_context()

        op = expr.op()

        # collect setup and teardown queries
        setup_queries = cls._generate_setup_queries(expr, context)
        teardown_queries = cls._generate_teardown_queries(expr, context)

        # TODO: any setup / teardown DDL statements will need to be done prior
        # to building the result set-generating statements.
        if isinstance(op, ops.Union):
            query = cls._make_union(expr, context)
        elif isinstance(op, ops.Intersection):
            query = cls._make_intersect(expr, context)
        elif isinstance(op, ops.Difference):
            query = cls._make_difference(expr, context)
        else:
            query = cls.select_builder_class().to_select(
                select_class=cls.select_class,
                table_set_formatter_class=cls.table_set_formatter_class,
                expr=expr,
                context=context,
                translator_class=cls.translator_class,
            )

        return QueryAST(
            context,
            query,
            setup_queries=setup_queries,
            teardown_queries=teardown_queries,
        )

    query_builder.Compiler._make_difference = _make_difference
    query_builder.Compiler._make_intersect = _make_intersect
    query_builder.Compiler._make_union = _make_union
    query_builder.Compiler.to_ast = to_ast

__all__ = ["Difference", "Intersection"]
