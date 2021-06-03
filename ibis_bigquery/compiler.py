"""Module to convert from Ibis expression to SQL string."""

from functools import partial

import ibis

try:
    import ibis.backends.base_sqlalchemy.compiler as comp
except ImportError:
    try:
        import ibis.sql.compiler as comp
    except ImportError:
        import ibis.backends.base.sql.compiler as comp

import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
import regex as re
import toolz

from .registry import _operation_registry


try:
    from ibis.backends.base_sql.compiler import (
        BaseExprTranslator,
        BaseSelect,
        BaseTableSetFormatter,
    )
except ImportError:
    # 1.2
    from ibis.impala.compiler import ImpalaExprTranslator as BaseExprTranslator
    from ibis.impala.compiler import ImpalaSelect as BaseSelect
    from ibis.impala.compiler import ImpalaTableSetFormatter as BaseTableSetFormatter


def build_ast(expr, context):
    """Create a QueryAST from an Ibis expression."""
    builder = BigQueryQueryBuilder(expr, context=context)
    return builder.get_result()


class BigQueryUDFNode(ops.ValueOp):
    """Represents use of a UDF."""


class BigQuerySelectBuilder(comp.SelectBuilder):
    """Transforms expression IR to a query pipeline."""

    @property
    def _select_class(self):
        return BigQuerySelect


class BigQueryUDFDefinition(comp.DDL):
    """Represents definition of a temporary UDF."""

    def __init__(self, expr, context):
        self.expr = expr
        self.context = context

    def compile(self):
        """Generate UDF string from definition."""
        return self.expr.op().js


class BigQueryUnion(comp.Union):
    """Union of tables."""

    @staticmethod
    def keyword(distinct):
        """Use disctinct UNION if distinct is True."""
        return "UNION DISTINCT" if distinct else "UNION ALL"


def find_bigquery_udf(expr):
    """Filter which includes only UDFs from expression tree."""
    if isinstance(expr.op(), BigQueryUDFNode):
        result = expr
    else:
        result = None
    return lin.proceed, result


class BigQueryQueryBuilder(comp.QueryBuilder):
    """Generator of QueryASTs."""

    select_builder = BigQuerySelectBuilder
    union_class = BigQueryUnion

    def generate_setup_queries(self):
        """Generate DDL for temporary resources."""
        queries = map(
            partial(BigQueryUDFDefinition, context=self.context),
            lin.traverse(find_bigquery_udf, self.expr),
        )

        # UDFs are uniquely identified by the name of the Node subclass we
        # generate.
        return list(toolz.unique(queries, key=lambda x: type(x.expr.op()).__name__))


class BigQueryContext(comp.QueryContext):
    """Recorder of information used in AST to SQL conversion."""

    def _to_sql(self, expr, ctx):
        builder = BigQueryQueryBuilder(expr, context=ctx)
        query_ast = builder.get_result()
        compiled = query_ast.compile()
        return compiled


class BigQueryExprTranslator(BaseExprTranslator):
    """Translate expressions to strings."""

    _registry = _operation_registry
    _rewrites = BaseExprTranslator._rewrites.copy()

    context_class = BigQueryContext

    def _trans_param(self, expr):
        op = expr.op()
        if op not in self.context.params:
            raise KeyError(op)
        return "@{}".format(expr.get_name())


try:
    compiles = BigQueryExprTranslator.compiles
except AttributeError:
    # https://github.com/ibis-project/ibis/commit/3d5a10
    def _add_operation(operation):
        def decorator(translation_func):
            BigQueryExprTranslator.add_operation(operation, translation_func)

        return decorator

    compiles = _add_operation

rewrites = BigQueryExprTranslator.rewrites


@rewrites(ops.DayOfWeekName)
def bigquery_day_of_week_name(e):
    """Convert TIMESTAMP to day-of-week string."""
    arg = e.op().args[0]
    return arg.strftime("%A")


class BigQueryTableSetFormatter(BaseTableSetFormatter):
    def _quote_identifier(self, name):
        if re.match(r"^[A-Za-z][A-Za-z_0-9]*$", name):
            return name
        return "`{}`".format(name)


class BigQuerySelect(BaseSelect):

    translator = BigQueryExprTranslator

    @property
    def table_set_formatter(self):
        return BigQueryTableSetFormatter


@rewrites(ops.IdenticalTo)
def identical_to(expr):
    left, right = expr.op().args
    return (left.isnull() & right.isnull()) | (left == right)


@rewrites(ops.Log2)
def log2(expr):
    (arg,) = expr.op().args
    return arg.log(2)


@rewrites(ops.Sum)
def bq_sum(expr):
    arg = expr.op().args[0]
    where = expr.op().args[1]
    if isinstance(arg, ir.BooleanColumn):
        return arg.cast("int64").sum(where=where)
    else:
        return expr


@rewrites(ops.Mean)
def bq_mean(expr):
    arg = expr.op().args[0]
    where = expr.op().args[1]
    if isinstance(arg, ir.BooleanColumn):
        return arg.cast("int64").mean(where=where)
    else:
        return expr


@rewrites(ops.Any)
@rewrites(ops.All)
@rewrites(ops.NotAny)
@rewrites(ops.NotAll)
def bigquery_any_all_no_op(expr):
    return expr
