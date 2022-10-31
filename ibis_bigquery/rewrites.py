"""Methods to translate BigQuery expressions before compilation."""

import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis.backends.base.sql import compiler as sql_compiler


def bigquery_day_of_week_name(e):
    """Convert TIMESTAMP to day-of-week string."""
    arg = e.op().args[0]
    return ops.Strftime(arg, "%A").to_expr()


def bq_floor_divide(expr):
    left, right = expr.op().args
    return ops.Floor(ops.Div(left, right)).to_expr()


def identical_to(expr):
    left, right = expr.op().args
    return ops.Or(
        ops.And(ops.IsNull(left).to_expr(), ops.IsNull(right).to_expr()).to_expr(),
        ops.Equals(left, right).to_expr(),
    ).to_expr()


def log2(expr):
    (arg,) = expr.op().args
    return ops.Log(arg, ops.Literal(2).to_expr()).to_expr()


def bq_sum(expr):
    arg = expr.op().args[0]
    where = expr.op().args[1]
    if isinstance(arg, ir.BooleanColumn):
        return arg.cast("int64").sum(where=where)
    else:
        return expr


def bq_mean(expr):
    arg = expr.op().args[0]
    where = expr.op().args[1]
    if isinstance(arg, ir.BooleanColumn):
        return arg.cast("int64").mean(where=where)
    else:
        return expr


def bigquery_any_all_no_op(expr):
    return expr


REWRITES = {
    **sql_compiler.ExprTranslator._rewrites,
    ops.DayOfWeekName: bigquery_day_of_week_name,
    ops.FloorDivide: bq_floor_divide,
    ops.IdenticalTo: identical_to,
    ops.Log2: log2,
    ops.Sum: bq_sum,
    ops.Mean: bq_mean,
    ops.Any: bigquery_any_all_no_op,
    ops.All: bigquery_any_all_no_op,
    ops.NotAny: bigquery_any_all_no_op,
    ops.NotAll: bigquery_any_all_no_op,
}
