"""BigQuery ibis client implementation."""

import datetime
from collections import OrderedDict
from typing import Tuple

import google.cloud.bigquery as bq
import ibis

try:
    import ibis.common.exceptions as com
except ImportError:
    import ibis.common as com

import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import pandas as pd
from google.api_core.client_info import ClientInfo
from ibis.backends.base import Database
from multipledispatch import Dispatcher

from .datatypes import ibis_type_to_bigquery_type

NATIVE_PARTITION_COL = "_PARTITIONTIME"


_DTYPE_TO_IBIS_TYPE = {
    "INT64": dt.int64,
    "FLOAT64": dt.double,
    "BOOL": dt.boolean,
    "STRING": dt.string,
    "DATE": dt.date,
    # FIXME: enforce no tz info
    "DATETIME": dt.timestamp,
    "TIME": dt.time,
    "TIMESTAMP": dt.timestamp,
    "BYTES": dt.binary,
    "NUMERIC": dt.Decimal(38, 9),
}


_LEGACY_TO_STANDARD = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}


_USER_AGENT_DEFAULT_TEMPLATE = "ibis/{}"


def _create_client_info(application_name):
    user_agent = []

    if application_name:
        user_agent.append(application_name)

    user_agent.append(_USER_AGENT_DEFAULT_TEMPLATE.format(ibis.__version__))
    return ClientInfo(user_agent=" ".join(user_agent))


@dt.dtype.register(bq.schema.SchemaField)
def bigquery_field_to_ibis_dtype(field):
    """Convert BigQuery `field` to an ibis type."""
    typ = field.field_type
    if typ == "RECORD":
        fields = field.fields
        assert fields, "RECORD fields are empty"
        names = [el.name for el in fields]
        ibis_types = list(map(dt.dtype, fields))
        ibis_type = dt.Struct(names, ibis_types)
    else:
        ibis_type = _LEGACY_TO_STANDARD.get(typ, typ)
        ibis_type = _DTYPE_TO_IBIS_TYPE.get(ibis_type, ibis_type)
    if field.mode == "REPEATED":
        ibis_type = dt.Array(ibis_type)
    return ibis_type


@sch.infer.register(bq.table.Table)
def bigquery_schema(table):
    """Infer the schema of a BigQuery `table` object."""
    fields = OrderedDict((el.name, dt.dtype(el)) for el in table.schema)
    partition_info = table._properties.get("timePartitioning", None)

    # We have a partitioned table
    if partition_info is not None:
        partition_field = partition_info.get("field", NATIVE_PARTITION_COL)

        # Only add a new column if it's not already a column in the schema
        fields.setdefault(partition_field, dt.timestamp)
    return sch.schema(fields)


class BigQueryCursor:
    """BigQuery cursor.

    This allows the BigQuery client to reuse machinery in
    :file:`ibis/client.py`.

    """

    def __init__(self, query):
        """Construct a BigQueryCursor with query `query`."""
        self.query = query

    def fetchall(self):
        """Fetch all rows."""
        result = self.query.result()
        return [row.values() for row in result]

    @property
    def columns(self):
        """Return the columns of the result set."""
        result = self.query.result()
        return [field.name for field in result.schema]

    @property
    def description(self):
        """Get the fields of the result set's schema."""
        result = self.query.result()
        return list(result.schema)

    def __enter__(self):
        # For compatibility when constructed from Query.execute()
        """No-op for compatibility.

        See Also
        --------
        ibis.client.Query.execute

        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """No-op for compatibility.

        See Also
        --------
        ibis.client.Query.execute

        """


def _find_scalar_parameter(expr):
    """Find all :class:`~ibis.expr.types.ScalarParameter` instances.

    Parameters
    ----------
    expr : ibis.expr.types.Expr

    Returns
    -------
    Tuple[bool, object]
        The operation and the parent expresssion's resolved name.

    """
    op = expr.op()

    if isinstance(op, ops.ScalarParameter):
        result = op, expr.get_name()
    else:
        result = None
    return lin.proceed, result


class BigQueryDatabase(Database):
    """A BigQuery dataset."""


bigquery_param = Dispatcher("bigquery_param")


@bigquery_param.register(ir.StructScalar, OrderedDict)
def bq_param_struct(param, value):
    field_params = [bigquery_param(param[k], v) for k, v in value.items()]
    result = bq.StructQueryParameter(param.get_name(), *field_params)
    return result


@bigquery_param.register(ir.ArrayValue, list)
def bq_param_array(param, value):
    param_type = param.type()
    assert isinstance(param_type, dt.Array), str(param_type)

    try:
        bigquery_type = ibis_type_to_bigquery_type(param_type.value_type)
    except NotImplementedError:
        raise com.UnsupportedBackendType(param_type)
    else:
        if isinstance(param_type.value_type, dt.Struct):
            query_value = [
                bigquery_param(param[i].name("element_{:d}".format(i)), struct)
                for i, struct in enumerate(value)
            ]
            bigquery_type = "STRUCT"
        elif isinstance(param_type.value_type, dt.Array):
            raise TypeError("ARRAY<ARRAY<T>> is not supported in BigQuery")
        else:
            query_value = value
        result = bq.ArrayQueryParameter(param.get_name(), bigquery_type, query_value)
        return result


@bigquery_param.register(ir.TimestampScalar, (str, datetime.datetime, datetime.date))
def bq_param_timestamp(param, value):
    assert isinstance(param.type(), dt.Timestamp), str(param.type())

    # TODO(phillipc): Not sure if this is the correct way to do this.
    timestamp_value = pd.Timestamp(value, tz="UTC").to_pydatetime()
    return bq.ScalarQueryParameter(param.get_name(), "TIMESTAMP", timestamp_value)


@bigquery_param.register(ir.StringScalar, str)
def bq_param_string(param, value):
    return bq.ScalarQueryParameter(param.get_name(), "STRING", value)


@bigquery_param.register(ir.IntegerScalar, int)
def bq_param_integer(param, value):
    return bq.ScalarQueryParameter(param.get_name(), "INT64", value)


@bigquery_param.register(ir.FloatingScalar, float)
def bq_param_double(param, value):
    return bq.ScalarQueryParameter(param.get_name(), "FLOAT64", value)


@bigquery_param.register(ir.BooleanScalar, bool)
def bq_param_boolean(param, value):
    return bq.ScalarQueryParameter(param.get_name(), "BOOL", value)


@bigquery_param.register(ir.DateScalar, str)
def bq_param_date_string(param, value):
    return bigquery_param(param, pd.Timestamp(value).to_pydatetime().date())


@bigquery_param.register(ir.DateScalar, datetime.datetime)
def bq_param_date_datetime(param, value):
    return bigquery_param(param, value.date())


@bigquery_param.register(ir.DateScalar, datetime.date)
def bq_param_date(param, value):
    return bq.ScalarQueryParameter(param.get_name(), "DATE", value)


class BigQueryTable(ops.DatabaseTable):
    pass


def rename_partitioned_column(table_expr, bq_table, partition_col):
    """Rename native partition column to user-defined name."""
    partition_info = bq_table._properties.get("timePartitioning", None)

    # If we don't have any partiton information, the table isn't partitioned
    if partition_info is None:
        return table_expr

    # If we have a partition, but no "field" field in the table properties,
    # then use NATIVE_PARTITION_COL as the default
    partition_field = partition_info.get("field", NATIVE_PARTITION_COL)

    # The partition field must be in table_expr columns
    assert partition_field in table_expr.columns

    # No renaming if the config option is set to None or the partition field
    # is not _PARTITIONTIME
    if partition_col is None or partition_field != NATIVE_PARTITION_COL:
        return table_expr
    return table_expr.relabel({NATIVE_PARTITION_COL: partition_col})


def parse_project_and_dataset(project: str, dataset: str = "") -> Tuple[str, str, str]:
    """Compute the billing project, data project, and dataset if available.

    This function figure out the project id under which queries will run versus
    the project of where the data live as well as what dataset to use.

    Parameters
    ----------
    project : str
        A project name
    dataset : Optional[str]
        A ``<project>.<dataset>`` string or just a dataset name

    Examples
    --------
    >>> data_project, billing_project, dataset = parse_project_and_dataset(
    ...     'ibis-gbq',
    ...     'foo-bar.my_dataset'
    ... )
    >>> data_project
    'foo-bar'
    >>> billing_project
    'ibis-gbq'
    >>> dataset
    'my_dataset'
    >>> data_project, billing_project, dataset = parse_project_and_dataset(
    ...     'ibis-gbq',
    ...     'my_dataset'
    ... )
    >>> data_project
    'ibis-gbq'
    >>> billing_project
    'ibis-gbq'
    >>> dataset
    'my_dataset'
    >>> data_project, billing_project, dataset = parse_project_and_dataset(
    ...     'ibis-gbq'
    ... )
    >>> data_project
    'ibis-gbq'
    >>> print(dataset)
    None

    """
    if dataset.count(".") > 1:
        raise ValueError(
            "{} is not a BigQuery dataset. More info https://cloud.google.com/bigquery/docs/datasets-intro".format(
                dataset
            )
        )
    elif dataset.count(".") == 1:
        data_project, dataset = dataset.split(".")
        billing_project = project
    else:
        billing_project = data_project = project

    return data_project, billing_project, dataset
