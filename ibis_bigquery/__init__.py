"""BigQuery public API."""

from typing import Optional

import google.auth.credentials
import google.cloud.bigquery  # noqa: F401, fail early if bigquery is missing
import pydata_google_auth
from pydata_google_auth import cache

from . import version as ibis_bigquery_version
from .client import BigQueryClient, BigQueryDatabase, BigQueryQuery, BigQueryTable
from .compiler import BigQueryExprTranslator, BigQueryQueryBuilder

try:
    from ibis.backends.base import BaseBackend
except ImportError:
    from .backcompat import BaseBackend

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


class Backend(BaseBackend):
    name = "bigquery"
    kind = "sql"
    builder = BigQueryQueryBuilder
    translator = BigQueryExprTranslator
    query_class = BigQueryQuery
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
    ) -> BigQueryClient:
        """Create a BigQueryClient for use with Ibis.

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
        BigQueryClient

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

        return BigQueryClient(
            backend=self,
            project_id=project_id,
            dataset_id=dataset_id,
            credentials=credentials,
            application_name=application_name,
            partition_column=partition_column,
        )


def compile(expr, params=None):
    """Compile an expression for BigQuery.
    Returns
    -------
    compiled : str
    See Also
    --------
    ibis.expr.types.Expr.compile
    """
    backend = Backend()
    return backend.compile(expr, params=params)


def connect(
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    credentials: Optional[google.auth.credentials.Credentials] = None,
    application_name: Optional[str] = None,
    auth_local_webserver: bool = False,
    auth_external_data: bool = False,
    auth_cache: str = "default",
    partition_column: Optional[str] = "PARTITIONTIME",
) -> BigQueryClient:
    """Create a BigQueryClient for use with Ibis.

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
    BigQueryClient

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
