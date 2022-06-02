"""Module to convert from Ibis expression to SQL string."""

from functools import partial

import ibis.expr.lineage as lin
import regex as re
import toolz
from ibis.backends.base.sql import compiler as sql_compiler

from ibis_bigquery import backports, operations, registry, rewrites


class BigQueryUDFDefinition(sql_compiler.DDL):
    """Represents definition of a temporary UDF."""

    def __init__(self, expr, context):
        self.expr = expr
        self.context = context

    def compile(self):
        """Generate UDF string from definition."""
        return self.expr.op().js


class BigQueryUnion(sql_compiler.Union):
    """Union of tables."""

    @staticmethod
    def keyword(distinct):
        """Use disctinct UNION if distinct is True."""
        return "UNION DISTINCT" if distinct else "UNION ALL"


class BigQueryIntersection(backports.Intersection):
    """Intersection of tables."""

    _keyword = "INTERSECT DISTINCT"


class BigQueryDifference(backports.Difference):
    """Difference of tables."""

    _keyword = "EXCEPT DISTINCT"


def find_bigquery_udf(expr):
    """Filter which includes only UDFs from expression tree."""
    if isinstance(expr.op(), operations.BigQueryUDFNode):
        result = expr
    else:
        result = None
    return lin.proceed, result


class BigQueryExprTranslator(sql_compiler.ExprTranslator):
    """Translate expressions to strings."""

    _registry = registry.OPERATION_REGISTRY
    _rewrites = rewrites.REWRITES

    @classmethod
    def compiles(cls, klass):
        def decorator(f):
            cls._registry[klass] = f
            return f

        return decorator

    def _trans_param(self, expr):
        op = expr.op()
        if op not in self.context.params:
            raise KeyError(op)
        return "@{}".format(expr.get_name())


compiles = BigQueryExprTranslator.compiles


class BigQueryTableSetFormatter(sql_compiler.TableSetFormatter):
    def _quote_identifier(self, name):
        if re.match(r"^[A-Za-z][A-Za-z_0-9]*$", name):
            return name
        return "`{}`".format(name)


class BigQueryCompiler(sql_compiler.Compiler):
    translator_class = BigQueryExprTranslator
    table_set_formatter_class = BigQueryTableSetFormatter
    union_class = BigQueryUnion
    intersect_class = BigQueryIntersection
    difference_class = BigQueryDifference

    @staticmethod
    def _generate_setup_queries(expr, context):
        """Generate DDL for temporary resources."""
        queries = map(
            partial(BigQueryUDFDefinition, context=context),
            lin.traverse(find_bigquery_udf, expr),
        )

        # UDFs are uniquely identified by the name of the Node subclass we
        # generate.
        return list(toolz.unique(queries, key=lambda x: type(x.expr.op()).__name__))
