import os
import pathlib

import google.auth
from ibis.backends.tests.base import BackendTest, RoundAwayFromZero, UnorderedComparator

import ibis_bigquery

DATASET_ID = "ibis_gbq_testing"
DEFAULT_PROJECT_ID = "ibis-gbq"
PROJECT_ID_ENV_VAR = "GOOGLE_BIGQUERY_PROJECT_ID"


class TestConf(UnorderedComparator, BackendTest, RoundAwayFromZero):
    """Backend-specific class with information for testing."""

    # These were moved from TestConf for use in common test suite.
    # TODO: Indicate RoundAwayFromZero and UnorderedComparator.
    # https://github.com/ibis-project/ibis-bigquery/issues/30
    supports_divide_by_zero = True
    supports_floating_modulus = False
    returned_timestamp_unit = "us"

    def name(self):
        """Name of the backend.
        In the parent class, this is automatically obtained from the name of
        the module, which is not the case for third-party backends.
        """
        return "bigquery"

    @staticmethod
    def connect(data_directory: pathlib.Path) -> ibis_bigquery.Backend:
        """Connect to the test database."""
        credentials, default_project_id = google.auth.default(
            scopes=ibis_bigquery.EXTERNAL_DATA_SCOPES
        )

        project_id = os.getenv(PROJECT_ID_ENV_VAR)
        if project_id is None:
            project_id = default_project_id
        if project_id is None:
            project_id = DEFAULT_PROJECT_ID

        return ibis_bigquery.connect(
            project_id=project_id,
            dataset_id=f"{project_id}.{DATASET_ID}",
            credentials=credentials,
        )
