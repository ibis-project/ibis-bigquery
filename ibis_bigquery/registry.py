import datetime
import base64
from functools import partial
import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.types as ir
import numpy as np
import regex as re
import toolz
import ibis

from multipledispatch import Dispatcher

from .datatypes import ibis_type_to_bigquery_type

try:
    import ibis.common.exceptions as com
except ImportError:
    import ibis.common as com

try:
    from ibis.backends.base.sql.registry import (
        fixed_arity,
        literal,
        operation_registry,
        reduction,
        unary,
    )
except ImportError:
    try:
        # 2.x
        from ibis.backends.base.sql import (
            fixed_arity,
            literal,
            operation_registry,
            reduction,
            unary,
        )
    except ImportError:
        try:
            # 1.4
            from ibis.backends.base_sql import (
                fixed_arity,
                literal,
                operation_registry,
                reduction,
                unary,
            )
        except ImportError:
            # 1.2
            from ibis.impala.compiler import _literal as literal
            from ibis.impala.compiler import _operation_registry as operation_registry
            from ibis.impala.compiler import _reduction as reduction
            from ibis.impala.compiler import fixed_arity, unary


def _struct_field(translator, expr):
    arg, field = expr.op().args
    arg_formatted = translator.translate(arg)
    return "{}.`{}`".format(arg_formatted, field)


def _array_concat(translator, expr):
    return "ARRAY_CONCAT({})".format(
        ", ".join(map(translator.translate, expr.op().args))
    )


def _array_index(translator, expr):
    # SAFE_OFFSET returns NULL if out of bounds
    return "{}[SAFE_OFFSET({})]".format(*map(translator.translate, expr.op().args))


def _hash(translator, expr):
    op = expr.op()
    arg, how = op.args

    arg_formatted = translator.translate(arg)

    if how == "farm_fingerprint":
        return f"farm_fingerprint({arg_formatted})"
    else:
        raise NotImplementedError(how)


def _string_find(translator, expr):
    haystack, needle, start, end = expr.op().args

    if start is not None:
        raise NotImplementedError("start not implemented for string find")
    if end is not None:
        raise NotImplementedError("end not implemented for string find")

    return "STRPOS({}, {}) - 1".format(
        translator.translate(haystack), translator.translate(needle)
    )


def _translate_pattern(translator, pattern):
    # add 'r' to string literals to indicate to BigQuery this is a raw string
    return "r" * isinstance(pattern.op(), ops.Literal) + translator.translate(pattern)


def _regex_search(translator, expr):
    arg, pattern = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = "REGEXP_CONTAINS({}, {})".format(translator.translate(arg), regex)
    return result


def _regex_extract(translator, expr):
    arg, pattern, index = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = "REGEXP_EXTRACT_ALL({}, {})[SAFE_OFFSET({})]".format(
        translator.translate(arg), regex, translator.translate(index)
    )
    return result


def _regex_replace(translator, expr):
    arg, pattern, replacement = expr.op().args
    regex = _translate_pattern(translator, pattern)
    result = "REGEXP_REPLACE({}, {}, {})".format(
        translator.translate(arg), regex, translator.translate(replacement)
    )
    return result


def _string_concat(translator, expr):
    return "CONCAT({})".format(", ".join(map(translator.translate, expr.op().arg)))


def _string_join(translator, expr):
    sep, args = expr.op().args
    return "ARRAY_TO_STRING([{}], {})".format(
        ", ".join(map(translator.translate, args)), translator.translate(sep)
    )


def _string_ascii(translator, expr):
    (arg,) = expr.op().args
    return "TO_CODE_POINTS({})[SAFE_OFFSET(0)]".format(translator.translate(arg))


def _string_right(translator, expr):
    arg, nchars = map(translator.translate, expr.op().args)
    return "SUBSTR({arg}, -LEAST(LENGTH({arg}), {nchars}))".format(
        arg=arg, nchars=nchars
    )


def _string_substring(translator, expr):
    op = expr.op()
    arg, start, length = op.args
    if length.op().value < 0:
        raise ValueError("Length parameter should not be a negative value.")

    base_substring = operation_registry[ops.Substring]
    base_substring(translator, expr)


def _array_literal_format(expr):
    return str(list(expr.op().value))


def _log(translator, expr):
    op = expr.op()
    arg, base = op.args
    arg_formatted = translator.translate(arg)

    if base is None:
        return "ln({})".format(arg_formatted)

    base_formatted = translator.translate(base)
    return "log({}, {})".format(arg_formatted, base_formatted)


def _literal(translator, expr):

    if isinstance(expr, ir.NumericValue):
        value = expr.op().value
        if not np.isfinite(value):
            return "CAST({!r} AS FLOAT64)".format(str(value))

    # special case literal timestamp, date, and time scalars
    if isinstance(expr.op(), ops.Literal):
        value = expr.op().value
        if isinstance(expr, ir.DateScalar):
            if isinstance(value, datetime.datetime):
                raw_value = value.date()
            else:
                raw_value = value
            return "DATE '{}'".format(raw_value)
        elif isinstance(expr, ir.TimestampScalar):
            return "TIMESTAMP '{}'".format(value)
        elif isinstance(expr, ir.TimeScalar):
            # TODO: define extractors on TimeValue expressions
            return "TIME '{}'".format(value)
        elif isinstance(expr, ir.BinaryScalar):
            return "FROM_BASE64('{}')".format(
                base64.b64encode(value).decode(encoding="utf-8")
            )

    try:
        return literal(translator, expr)
    except NotImplementedError:
        if isinstance(expr, ir.ArrayValue):
            return _array_literal_format(expr)
        raise NotImplementedError(type(expr).__name__)


def _arbitrary(translator, expr):
    arg, how, where = expr.op().args

    if where is not None:
        arg = where.ifelse(arg, ibis.NA)

    if how not in (None, "first"):
        raise com.UnsupportedOperationError(
            "{!r} value not supported for arbitrary in BigQuery".format(how)
        )

    return "ANY_VALUE({})".format(translator.translate(arg))


_date_units = {
    "Y": "YEAR",
    "Q": "QUARTER",
    "W": "WEEK",
    "M": "MONTH",
    "D": "DAY",
}


_timestamp_units = {
    "us": "MICROSECOND",
    "ms": "MILLISECOND",
    "s": "SECOND",
    "m": "MINUTE",
    "h": "HOUR",
}
_time_units = _timestamp_units.copy()
_timestamp_units.update(_date_units)


def _truncate(kind, units):
    def truncator(translator, expr):
        arg, unit = expr.op().args
        trans_arg = translator.translate(arg)
        valid_unit = units.get(unit)
        if valid_unit is None:
            raise com.UnsupportedOperationError(
                "BigQuery does not support truncating {} values to unit "
                "{!r}".format(arg.type(), unit)
            )
        return "{}_TRUNC({}, {})".format(kind, trans_arg, valid_unit)

    return truncator


def _timestamp_op(func, units):
    def _formatter(translator, expr):
        op = expr.op()
        arg, offset = op.args

        unit = offset.type().unit
        if unit not in units:
            raise com.UnsupportedOperationError(
                "BigQuery does not allow binary operation "
                "{} with INTERVAL offset {}".format(func, unit)
            )
        formatted_arg = translator.translate(arg)
        formatted_offset = translator.translate(offset)
        result = "{}({}, {})".format(func, formatted_arg, formatted_offset)
        return result

    return _formatter


def _extract_field(sql_attr):
    def extract_field_formatter(translator, expr):
        op = expr.op()
        arg = translator.translate(op.args[0])
        if sql_attr == "epochseconds":
            return f"UNIX_SECONDS({arg})"
        else:
            return f"EXTRACT({sql_attr} from {arg})"

    return extract_field_formatter


STRFTIME_FORMAT_FUNCTIONS = {
    dt.Date: "DATE",
    dt.Time: "TIME",
    dt.Timestamp: "TIMESTAMP",
}


bigquery_cast = Dispatcher("bigquery_cast")


@bigquery_cast.register(str, dt.Timestamp, dt.Integer)
def bigquery_cast_timestamp_to_integer(compiled_arg, from_, to):
    """Convert TIMESTAMP to INT64 (seconds since Unix epoch)."""
    return "UNIX_MICROS({})".format(compiled_arg)


@bigquery_cast.register(str, dt.DataType, dt.DataType)
def bigquery_cast_generate(compiled_arg, from_, to):
    """Cast to desired type."""
    sql_type = ibis_type_to_bigquery_type(to)
    return "CAST({} AS {})".format(compiled_arg, sql_type)


def _cast(translator, expr):
    op = expr.op()
    arg, target_type = op.args
    arg_formatted = translator.translate(arg)
    return bigquery_cast(arg_formatted, arg.type(), target_type)


def bigquery_day_of_week_index(t, e):
    """Convert timestamp to day-of-week integer."""
    arg = e.op().args[0]
    arg_formatted = t.translate(arg)
    return "MOD(EXTRACT(DAYOFWEEK FROM {}) + 5, 7)".format(arg_formatted)


def bigquery_compiles_divide(t, e):
    """Floating point division."""
    return "IEEE_DIVIDE({}, {})".format(*map(t.translate, e.op().args))


def compiles_strftime(translator, expr):
    """Timestamp formatting."""
    arg, format_string = expr.op().args
    arg_type = arg.type()
    strftime_format_func_name = STRFTIME_FORMAT_FUNCTIONS[type(arg_type)]
    fmt_string = translator.translate(format_string)
    arg_formatted = translator.translate(arg)
    if isinstance(arg_type, dt.Timestamp):
        return "FORMAT_{}({}, {}, {!r})".format(
            strftime_format_func_name,
            fmt_string,
            arg_formatted,
            arg_type.timezone if arg_type.timezone is not None else "UTC",
        )
    return "FORMAT_{}({}, {})".format(
        strftime_format_func_name, fmt_string, arg_formatted
    )


def compiles_string_to_timestamp(translator, expr):
    """Timestamp parsing."""
    arg, format_string, timezone_arg = expr.op().args
    fmt_string = translator.translate(format_string)
    arg_formatted = translator.translate(arg)
    if timezone_arg is not None:
        timezone_str = translator.translate(timezone_arg)
        return "PARSE_TIMESTAMP({}, {}, {})".format(
            fmt_string, arg_formatted, timezone_str
        )
    return "PARSE_TIMESTAMP({}, {})".format(fmt_string, arg_formatted)


UNIT_FUNCS = {"s": "SECONDS", "ms": "MILLIS", "us": "MICROS"}


def compiles_timestamp_from_unix(t, e):
    value, unit = e.op().args
    return "TIMESTAMP_{}({})".format(UNIT_FUNCS[unit], t.translate(value))


def compiles_floor(t, e):
    bigquery_type = ibis_type_to_bigquery_type(e.type())
    arg = e.op().arg
    return "CAST(FLOOR({}) AS {})".format(t.translate(arg), bigquery_type)


def compiles_approx(translator, expr):
    expr = expr.op()
    arg = expr.arg
    where = expr.where

    if where is not None:
        arg = where.ifelse(arg, ibis.NA)

    return "APPROX_QUANTILES({}, 2)[OFFSET(1)]".format(translator.translate(arg))


def compiles_covar(translator, expr):
    expr = expr.op()
    left = expr.left
    right = expr.right
    where = expr.where

    if expr.how == "sample":
        how = "SAMP"
    elif expr.how == "pop":
        how = "POP"
    else:
        raise ValueError("Covariance with how={!r} is not supported.".format(how))

    if where is not None:
        left = where.ifelse(left, ibis.NA)
        right = where.ifelse(right, ibis.NA)

    return "COVAR_{}({}, {})".format(
        how, translator.translate(left), translator.translate(right)
    )


def bigquery_compile_any(translator, expr):
    return "LOGICAL_OR({})".format(*map(translator.translate, expr.op().args))


def bigquery_compile_notany(translator, expr):
    return "LOGICAL_AND(NOT ({}))".format(*map(translator.translate, expr.op().args))


def bigquery_compile_all(translator, expr):
    return "LOGICAL_AND({})".format(*map(translator.translate, expr.op().args))


def bigquery_compile_notall(translator, expr):
    return "LOGICAL_OR(NOT ({}))".format(*map(translator.translate, expr.op().args))


_operation_registry = {
    **operation_registry,
}


_operation_registry.update(
    {
        ops.All: bigquery_compile_all,
        ops.NotAll: bigquery_compile_notall,
        ops.Any: bigquery_compile_any,
        ops.NotAny: bigquery_compile_notany,
        ops.StringToTimestamp: compiles_string_to_timestamp,
        ops.TimestampFromUNIX: compiles_timestamp_from_unix,
        ops.Strftime: compiles_strftime,
        ops.Floor: compiles_floor,
        ops.Covariance: compiles_covar,
        ops.CMSMedian: compiles_approx,
        ops.DayOfWeekIndex: bigquery_day_of_week_index,
        ops.Divideops.Divide: bigquery_compiles_divide,
        ops.ExtractYear: _extract_field("year"),
        ops.ExtractMonth: _extract_field("month"),
        ops.ExtractDay: _extract_field("day"),
        ops.ExtractHour: _extract_field("hour"),
        ops.ExtractMinute: _extract_field("minute"),
        ops.ExtractSecond: _extract_field("second"),
        ops.ExtractMillisecond: _extract_field("millisecond"),
        ops.Hash: _hash,
        ops.StringReplace: fixed_arity("REPLACE", 3),
        ops.StringSplit: fixed_arity("SPLIT", 2),
        ops.StringConcat: _string_concat,
        ops.StringJoin: _string_join,
        ops.StringAscii: _string_ascii,
        ops.StringFind: _string_find,
        ops.Substring: _string_substring,
        ops.StrRight: _string_right,
        ops.Repeat: fixed_arity("REPEAT", 2),
        ops.RegexSearch: _regex_search,
        ops.RegexExtract: _regex_extract,
        ops.RegexReplace: _regex_replace,
        ops.GroupConcat: reduction("STRING_AGG"),
        ops.IfNull: fixed_arity("IFNULL", 2),
        ops.Cast: _cast,
        ops.StructField: _struct_field,
        ops.ArrayCollect: unary("ARRAY_AGG"),
        ops.ArrayConcat: _array_concat,
        ops.ArrayIndex: _array_index,
        ops.ArrayLength: unary("ARRAY_LENGTH"),
        ops.HLLCardinality: reduction("APPROX_COUNT_DISTINCT"),
        ops.Log: _log,
        ops.Sign: unary("SIGN"),
        ops.Modulus: fixed_arity("MOD", 2),
        ops.Date: unary("DATE"),
        # BigQuery doesn't have these operations built in.
        # ops.ArrayRepeat: _array_repeat,
        # ops.ArraySlice: _array_slice,
        ops.Literal: _literal,
        ops.Arbitrary: _arbitrary,
        ops.TimestampTruncate: _truncate("TIMESTAMP", _timestamp_units),
        ops.DateTruncate: _truncate("DATE", _date_units),
        ops.TimeTruncate: _truncate("TIME", _timestamp_units),
        ops.Time: unary("TIME"),
        ops.TimestampAdd: _timestamp_op("TIMESTAMP_ADD", {"h", "m", "s", "ms", "us"}),
        ops.TimestampSub: _timestamp_op("TIMESTAMP_SUB", {"h", "m", "s", "ms", "us"}),
        ops.DateAdd: _timestamp_op("DATE_ADD", {"D", "W", "M", "Q", "Y"}),
        ops.DateSub: _timestamp_op("DATE_SUB", {"D", "W", "M", "Q", "Y"}),
        ops.TimestampNow: fixed_arity("CURRENT_TIMESTAMP", 0),
    }
)


def _try_register_op(op_name: str, value):
    """Register operation if it exists in Ibis.

    This allows us to decouple slightly from ibis-framework releases.
    """
    if hasattr(ops, op_name):
        _operation_registry[getattr(ops, op_name)] = value


# 2.x
_try_register_op("BitAnd", reduction("BIT_AND"))
_try_register_op("BitOr", reduction("BIT_OR"))
_try_register_op("BitXor", reduction("BIT_XOR"))
# 1.4
_try_register_op("ExtractQuarter", _extract_field("quarter"))
_try_register_op("ExtractEpochSeconds", _extract_field("epochseconds"))


_invalid_operations = {
    ops.Translate,
    ops.FindInSet,
    ops.Capitalize,
    ops.DateDiff,
    ops.TimestampDiff,
}

_operation_registry = {
    k: v for k, v in _operation_registry.items() if k not in _invalid_operations
}
