import os
from google.auth.environment_vars import PROJECT

import ibis  # noqa: F401
import pytest
import google.auth
import google.auth.exceptions

import ibis_bigquery

DEFAULT_PROJECT_ID = "ibis-gbq"
PROJECT_ID_ENV_VAR = "GOOGLE_BIGQUERY_PROJECT_ID"
DATASET_ID = "testing"

bq = ibis_bigquery.Backend()


@pytest.fixture(scope="session")
def default_credentials():
    try:
        credentials, project_id = google.auth.default(scopes=ibis_bigquery.EXTERNAL_DATA_SCOPES)
    except google.auth.excecptions.DefaultCredentialsError as exc:
        pytest.skip(f"Could not get GCP credentials: {exc}")
    
    return credentials, project_id


@pytest.fixture(scope="session")
def project_id(default_credentials):
    project_id = os.getenv(PROJECT_ID_ENV_VAR)
    if project_id is None:
        _, project_id = default_credentials
    if project_id is None:
        project_id = DEFAULT_PROJECT_ID
    return project_id


@pytest.fixture(scope="session")
def credentials(default_credentials):
    credentials, _ = default_credentials
    return credentials


@pytest.fixture(scope="session")
def client(credentials, project_id):
    return bq.connect(
        project_id=project_id, dataset_id=DATASET_ID, credentials=credentials,
    )


@pytest.fixture(scope="session")
def client2(credentials, project_id):
    return bq.connect(
        project_id=project_id, dataset_id=DATASET_ID, credentials=credentials,
    )


@pytest.fixture(scope="session")
def alltypes(client):
    return client.table("functional_alltypes")


@pytest.fixture(scope="session")
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope="session")
def parted_alltypes(client):
    return client.table("functional_alltypes_parted")


@pytest.fixture(scope="session")
def parted_df(parted_alltypes):
    return parted_alltypes.execute()


@pytest.fixture(scope="session")
def struct_table(client):
    return client.table("struct_table")


@pytest.fixture(scope="session")
def numeric_table(client):
    return client.table("numeric_table")


@pytest.fixture(scope="session")
def public(project_id, credentials):
    return bq.connect(
        project_id=project_id,
        dataset_id="bigquery-public-data.stackoverflow",
        credentials=credentials,
    )
