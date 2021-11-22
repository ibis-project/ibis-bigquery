"""BigQuery public API."""
from typing import Optional

import google.auth.credentials
import google.cloud.bigquery as bq
from google.api_core.exceptions import NotFound
import pydata_google_auth
from pydata_google_auth import cache

from ibis.backends.base.sql import BaseSQLBackend
import ibis.expr.schema as sch
import ibis.expr.types as ir

from . import version as ibis_bigquery_version
from .client import (
    BigQueryDatabase, BigQueryTable,
    parse_project_and_dataset, _create_client_info,
    rename_partitioned_column, bigquery_field_to_ibis_dtype,
    BigQueryCursor)
from .compiler import BigQueryCompiler

try:
    from .udf import udf  # noqa F401
except ImportError:
    pass


__version__: str = ibis_bigquery_version.__version__

SCOPES = ["https://www.googleapis.com/auth/bigquery"]
EXTERNAL_DATA_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
]
CLIENT_ID = "546535678771-gvffde27nd83kfl6qbrnletqvkdmsese.apps.googleusercontent.com"
CLIENT_SECRET = "iU5ohAF2qcqrujegE3hQ1cPt"


class Backend(BaseSQLBackend):
    name = "bigquery"
    compiler = BigQueryCompiler
    database_class = BigQueryDatabase
    table_class = BigQueryTable

    # These were moved from TestConf for use in common test suite.
    # TODO: Indicate RoundAwayFromZero and UnorderedComparator.
    # https://github.com/ibis-project/ibis-bigquery/issues/30
    supports_divide_by_zero = True
    supports_floating_modulus = False
    returned_timestamp_unit = "us"

    def connect(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        credentials: Optional[google.auth.credentials.Credentials] = None,
        application_name: Optional[str] = None,
        auth_local_webserver: bool = False,
        auth_external_data: bool = False,
        auth_cache: str = "default",
        partition_column: Optional[str] = "PARTITIONTIME",
    ) -> "Backend":
        """Create a :class:`Backend` for use with Ibis.

        Parameters
        ----------
        project_id : str
            A BigQuery project id.
        dataset_id : str
            A dataset id that lives inside of the project indicated by
            `project_id`.
        credentials : google.auth.credentials.Credentials
        application_name : str
            A string identifying your application to Google API endpoints.
        auth_local_webserver : bool
            Use a local webserver for the user authentication.  Binds a
            webserver to an open port on localhost between 8080 and 8089,
            inclusive, to receive authentication token. If not set, defaults
            to False, which requests a token via the console.
        auth_external_data : bool
            Authenticate using additional scopes required to `query external
            data sources
            <https://cloud.google.com/bigquery/external-data-sources>`_,
            such as Google Sheets, files in Google Cloud Storage, or files in
            Google Drive. If not set, defaults to False, which requests the
            default BigQuery scopes.
        auth_cache : str
            Selects the behavior of the credentials cache.

            ``'default'``
                Reads credentials from disk if available, otherwise
                authenticates and caches credentials to disk.

            ``'reauth'``
                Authenticates and caches credentials to disk.

            ``'none'``
                Authenticates and does **not** cache credentials.

            Defaults to ``'default'``.
        partition_column : str
            Identifier to use instead of default ``_PARTITIONTIME`` partition
            column. Defaults to ``'PARTITIONTIME'``.

        Returns
        -------
        Backend

        """
        default_project_id = None

        if credentials is None:
            scopes = SCOPES
            if auth_external_data:
                scopes = EXTERNAL_DATA_SCOPES

            if auth_cache == "default":
                credentials_cache = cache.ReadWriteCredentialsCache(
                    filename="ibis.json"
                )
            elif auth_cache == "reauth":
                credentials_cache = cache.WriteOnlyCredentialsCache(
                    filename="ibis.json"
                )
            elif auth_cache == "none":
                credentials_cache = cache.NOOP
            else:
                raise ValueError(
                    f"Got unexpected value for auth_cache = '{auth_cache}'. "
                    "Expected one of 'default', 'reauth', or 'none'."
                )

            credentials, default_project_id = pydata_google_auth.default(
                scopes,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                credentials_cache=credentials_cache,
                use_local_webserver=auth_local_webserver,
            )

        project_id = project_id or default_project_id

        new_backend = self.__class__()

        (
            new_backend.data_project,
            new_backend.billing_project,
            new_backend.dataset,
        ) = parse_project_and_dataset(project_id, dataset_id)

        new_backend.client = bq.Client(
            project=new_backend.billing_project,
            credentials=credentials,
            client_info=_create_client_info(application_name),
        )
        new_backend.partition_column = partition_column

        return new_backend

    def _parse_project_and_dataset(self, dataset):
        if not dataset and not self.dataset:
            raise ValueError("Unable to determine BigQuery dataset.")
        project, _, dataset = parse_project_and_dataset(
            self.billing_project,
            dataset or "{}.{}".format(self.data_project, self.dataset),
        )
        return project, dataset

    @property
    def project_id(self):
        return self.data_project

    @property
    def dataset_id(self):
        return self.dataset

    def table(self, name, database=None) -> ir.TableExpr:
        t = super().table(name, database=database)
        project, dataset, name = t.op().name.split(".")
        dataset_ref = self.client.dataset(dataset, project=project)
        table_ref = dataset_ref.table(name)
        bq_table = self.client.get_table(table_ref)
        return rename_partitioned_column(t, bq_table, self.partition_column)

    def _fully_qualified_name(self, name, database):
        project, dataset = self._parse_project_and_dataset(database)
        return "{}.{}.{}".format(project, dataset, name)

    def _get_schema_using_query(self, limited_query):
        with self._execute(limited_query, results=True) as cur:
            # resets the state of the cursor and closes operation
            names, ibis_types = self._adapt_types(cur.description)
        return sch.Schema(names, ibis_types)

    def _get_table_schema(self, qualified_name):
        dataset, table = qualified_name.rsplit(".", 1)
        assert dataset is not None, "dataset is None"
        return self.get_schema(table, database=dataset)

    def _get_query(self, dml, **kwargs):
        return self.query_class(self, dml, query_parameters=dml.context.params)

    def _adapt_types(self, descr):
        names = []
        adapted_types = []
        for col in descr:
            names.append(col.name)
            typename = bigquery_field_to_ibis_dtype(col)
            adapted_types.append(typename)
        return names, adapted_types

    def _execute(self, stmt, results=True, query_parameters=None):
        job_config = bq.job.QueryJobConfig()
        job_config.query_parameters = query_parameters or []
        job_config.use_legacy_sql = False  # False by default in >=0.28
        query = self.client.query(
            stmt, job_config=job_config, project=self.billing_project
        )
        query.result()  # blocks until finished
        return BigQueryCursor(query)

    @property
    def current_database(self):
        return self.database(self.dataset)

    def database(self, name=None):
        if name is None and self.dataset is None:
            raise ValueError(
                "Unable to determine BigQuery dataset. Call "
                "client.database('my_dataset') or set_database('my_dataset') "
                "to assign your client a dataset."
            )
        return self.database_class(name or self.dataset, self)

    def exists_database(self, name):
        project, dataset = self._parse_project_and_dataset(name)
        client = self.client
        dataset_ref = client.dataset(dataset, project=project)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            return False
        else:
            return True

    def fetch_from_cursor(self, cursor, schema):
        df = cursor.query.to_dataframe()
        return schema.apply_to(df)

    def get_schema(self, name, database=None):
        project, dataset = self._parse_project_and_dataset(database)
        table_ref = self.client.dataset(dataset, project=project).table(name)
        bq_table = self.client.get_table(table_ref)
        return sch.infer(bq_table)

    def list_databases(self, like=None):
        results = [
            dataset.dataset_id
            for dataset in self.client.list_datasets(project=self.data_project)
        ]
        return self._filter_with_like(results, like)

    def list_tables(self, like=None, database=None):
        project, dataset = self._parse_project_and_dataset(database)
        dataset_ref = bq.DatasetReference(project, dataset)
        result = [table.table_id for table in self.client.list_tables(dataset_ref)]
        return self._filter_with_like(result, like)

    def set_database(self, name):
        self.data_project, self.dataset = self._parse_project_and_dataset(name)

    @property
    def version(self):
        return bq.__version__


def compile(expr, params=None, **kwargs):
    """Compile an expression for BigQuery.
    Returns
    -------
    compiled : str
    See Also
    --------
    ibis.expr.types.Expr.compile
    """
    backend = Backend()
    return backend.compile(expr, params=params, **kwargs)


def connect(
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    credentials: Optional[google.auth.credentials.Credentials] = None,
    application_name: Optional[str] = None,
    auth_local_webserver: bool = False,
    auth_external_data: bool = False,
    auth_cache: str = "default",
    partition_column: Optional[str] = "PARTITIONTIME",
) -> Backend:
    """Create a :class:`Backend` for use with Ibis.

    Parameters
    ----------
    project_id : str
        A BigQuery project id.
    dataset_id : str
        A dataset id that lives inside of the project indicated by
        `project_id`.
    credentials : google.auth.credentials.Credentials
    application_name : str
        A string identifying your application to Google API endpoints.
    auth_local_webserver : bool
        Use a local webserver for the user authentication.  Binds a
        webserver to an open port on localhost between 8080 and 8089,
        inclusive, to receive authentication token. If not set, defaults
        to False, which requests a token via the console.
    auth_external_data : bool
        Authenticate using additional scopes required to `query external
        data sources
        <https://cloud.google.com/bigquery/external-data-sources>`_,
        such as Google Sheets, files in Google Cloud Storage, or files in
        Google Drive. If not set, defaults to False, which requests the
        default BigQuery scopes.
    auth_cache : str
        Selects the behavior of the credentials cache.

        ``'default'``
            Reads credentials from disk if available, otherwise
            authenticates and caches credentials to disk.

        ``'reauth'``
            Authenticates and caches credentials to disk.

        ``'none'``
            Authenticates and does **not** cache credentials.

        Defaults to ``'default'``.
    partition_column : str
        Identifier to use instead of default ``_PARTITIONTIME`` partition
        column. Defaults to ``'PARTITIONTIME'``.

    Returns
    -------
    Backend

    """
    backend = Backend()
    return backend.connect(
        project_id=project_id,
        dataset_id=dataset_id,
        credentials=credentials,
        application_name=application_name,
        auth_local_webserver=auth_local_webserver,
        auth_external_data=auth_external_data,
        auth_cache=auth_cache,
        partition_column=partition_column,
    )


__all__ = [
    "__version__",
    "Backend",
    "compile",
    "connect",
]
