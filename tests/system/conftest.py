import io
import os
import shutil
import tempfile
import urllib.request

import google.auth
import google.auth.exceptions
import ibis  # noqa: F401
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

import ibis_bigquery

DEFAULT_PROJECT_ID = "ibis-gbq"
PROJECT_ID_ENV_VAR = "GOOGLE_BIGQUERY_PROJECT_ID"
DATASET_ID = "ibis_gbq_testing"
TESTING_DATA_URI = "https://raw.githubusercontent.com/ibis-project/testing-data/master"

bq = ibis_bigquery.Backend()


def pytest_addoption(parser):
    parser.addoption(
        "--save-dataset",
        action="store_true",
        default=False,
        help="saves all test data in the testing dataset",
    )
    parser.addoption(
        "--no-refresh-dataset",
        action="store_true",
        default=False,
        help="do not refresh the test data in the testing dataset",
    )


@pytest.fixture(scope="session")
def dataset_id() -> str:
    return DATASET_ID


@pytest.fixture(scope="session")
def default_credentials():
    try:
        credentials, project_id = google.auth.default(
            scopes=ibis_bigquery.EXTERNAL_DATA_SCOPES
        )
    except google.auth.exceptions.DefaultCredentialsError as exc:
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
        project_id=project_id,
        dataset_id=DATASET_ID,
        credentials=credentials,
    )


@pytest.fixture(scope="session")
def client2(credentials, project_id):
    return bq.connect(
        project_id=project_id,
        dataset_id=DATASET_ID,
        credentials=credentials,
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


# Native BigQuery client fixtures
# required to dynamically create the testing dataset,
# the tables, and to populate data into the tables.
@pytest.fixture(scope="session")
def bqclient(client):
    return client.client


# Create testing dataset.
@pytest.fixture(autouse=True, scope="session")
def testing_dataset(bqclient, request, dataset_id):
    dataset_ref = bigquery.DatasetReference(bqclient.project, dataset_id)
    try:
        bqclient.create_dataset(dataset_ref, exists_ok=True)
    except NotFound:
        pass
    yield dataset_ref
    if not request.config.getoption("--save-dataset"):
        bqclient.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)


@pytest.fixture(scope="session")
def functional_alltypes_table(testing_dataset):
    return bigquery.TableReference(testing_dataset, "functional_alltypes")


@pytest.fixture(autouse=True, scope="session")
def create_functional_alltypes_table(bqclient, functional_alltypes_table):
    table = bigquery.Table(functional_alltypes_table)
    table.schema = [
        bigquery.SchemaField("index", "INTEGER"),
        bigquery.SchemaField("Unnamed_0", "INTEGER"),
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("bool_col", "BOOLEAN"),
        bigquery.SchemaField("tinyint_col", "INTEGER"),
        bigquery.SchemaField("smallint_col", "INTEGER"),
        bigquery.SchemaField("int_col", "INTEGER"),
        bigquery.SchemaField("bigint_col", "INTEGER"),
        bigquery.SchemaField("float_col", "FLOAT"),
        bigquery.SchemaField("double_col", "FLOAT"),
        bigquery.SchemaField("date_string_col", "STRING"),
        bigquery.SchemaField("string_col", "STRING"),
        bigquery.SchemaField("timestamp_col", "TIMESTAMP"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
    ]
    bqclient.create_table(table, exists_ok=True)
    return table


@pytest.fixture(autouse=True, scope="session")
def load_functional_alltypes_data(request, bqclient, create_functional_alltypes_table):
    if request.config.getoption("--no-refresh-dataset"):
        return

    table = create_functional_alltypes_table
    load_config = bigquery.LoadJobConfig()
    load_config.skip_leading_rows = 1  # skip the header row.
    load_config.write_disposition = "WRITE_TRUNCATE"
    filepath = download_file("{}/functional_alltypes.csv".format(TESTING_DATA_URI))
    with open(filepath.name, "rb") as csvfile:
        job = bqclient.load_table_from_file(
            csvfile,
            table,
            job_config=load_config,
        ).result()
    if job.error_result:
        print("error")


# Ingestion time partitioned table.
@pytest.fixture(scope="session")
def functional_alltypes_parted_table(testing_dataset):
    return bigquery.TableReference(testing_dataset, "functional_alltypes_parted")


@pytest.fixture(scope="session")
def create_functional_alltypes_parted_table(bqclient, functional_alltypes_parted_table):
    table = bigquery.Table(functional_alltypes_parted_table)
    table.schema = [
        bigquery.SchemaField("index", "INTEGER"),
        bigquery.SchemaField("Unnamed_0", "INTEGER"),
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("bool_col", "BOOLEAN"),
        bigquery.SchemaField("tinyint_col", "INTEGER"),
        bigquery.SchemaField("smallint_col", "INTEGER"),
        bigquery.SchemaField("int_col", "INTEGER"),
        bigquery.SchemaField("bigint_col", "INTEGER"),
        bigquery.SchemaField("float_col", "FLOAT"),
        bigquery.SchemaField("double_col", "FLOAT"),
        bigquery.SchemaField("date_string_col", "STRING"),
        bigquery.SchemaField("string_col", "STRING"),
        bigquery.SchemaField("timestamp_col", "TIMESTAMP"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
    ]
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY
    )
    table.require_partition_filter = False
    bqclient.create_table(table, exists_ok=True)
    return table


@pytest.fixture(autouse=True, scope="session")
def load_functional_alltypes_parted_data(
    request, bqclient, create_functional_alltypes_parted_table
):
    if request.config.getoption("--no-refresh-dataset"):
        return

    table = create_functional_alltypes_parted_table
    load_config = bigquery.LoadJobConfig()
    load_config.write_disposition = "WRITE_TRUNCATE"
    load_config.skip_leading_rows = 1  # skip the header row.
    filepath = download_file("{}/functional_alltypes.csv".format(TESTING_DATA_URI))
    with open(filepath.name, "rb") as csvfile:
        job = bqclient.load_table_from_file(
            csvfile,
            table,
            job_config=load_config,
        ).result()
    if job.error_result:
        print("error")


# Create a table with complex data types (nested and repeated).
@pytest.fixture(scope="session")
def struct_bq_table(testing_dataset):
    return bigquery.TableReference(testing_dataset, "struct_table")


@pytest.fixture(autouse=True, scope="session")
def load_struct_table_data(request, bqclient, struct_bq_table):
    if request.config.getoption("--no-refresh-dataset"):
        return

    load_config = bigquery.LoadJobConfig()
    load_config.write_disposition = "WRITE_TRUNCATE"
    load_config.source_format = "AVRO"
    filepath = download_file("{}/struct_table.avro".format(TESTING_DATA_URI))
    with open(filepath.name, "rb") as avrofile:
        job = bqclient.load_table_from_file(
            avrofile,
            struct_bq_table,
            job_config=load_config,
        ).result()
    if job.error_result:
        print("error")


# Create empty date-partitioned table.
@pytest.fixture(scope="session")
def date_table(testing_dataset):
    return bigquery.TableReference(testing_dataset, "date_column_parted")


@pytest.fixture(autouse=True, scope="session")
def create_date_table(bqclient, date_table):
    table = bigquery.Table(date_table)
    table.schema = [
        bigquery.SchemaField("my_date_parted_col", "DATE"),
        bigquery.SchemaField("string_col", "STRING"),
        bigquery.SchemaField("int_col", "INTEGER"),
    ]
    table.time_partitioning = bigquery.TimePartitioning(field="my_date_parted_col")
    bqclient.create_table(table, exists_ok=True)
    return table


# Create empty timestamp-partitioned tables.
@pytest.fixture(scope="session")
def timestamp_table(testing_dataset):
    return bigquery.TableReference(testing_dataset, "timestamp_column_parted")


@pytest.fixture(autouse=True, scope="session")
def create_timestamp_table(bqclient, timestamp_table):
    table = bigquery.Table(timestamp_table)
    table.schema = [
        bigquery.SchemaField("my_timestamp_parted_col", "DATE"),
        bigquery.SchemaField("string_col", "STRING"),
        bigquery.SchemaField("int_col", "INTEGER"),
    ]
    table.time_partitioning = bigquery.TimePartitioning(field="my_timestamp_parted_col")
    bqclient.create_table(table, exists_ok=True)


# Create a table with a numeric column
@pytest.fixture(scope="session")
def numeric_bq_table(testing_dataset):
    return bigquery.TableReference(testing_dataset, "numeric_table")


@pytest.fixture(scope="session")
def create_numeric_table(bqclient, numeric_bq_table):
    table = bigquery.Table(numeric_bq_table)
    table.schema = [
        bigquery.SchemaField("string_col", "STRING"),
        bigquery.SchemaField("numeric_col", "NUMERIC"),
    ]
    bqclient.create_table(table, exists_ok=True)
    return table


@pytest.fixture(autouse=True, scope="session")
def load_numeric_data(request, bqclient, create_numeric_table):
    if request.config.getoption("--no-refresh-dataset"):
        return

    load_config = bigquery.LoadJobConfig()
    load_config.write_disposition = "WRITE_TRUNCATE"
    load_config.source_format = "NEWLINE_DELIMITED_JSON"
    data = """{"string_col": "1st value", "numeric_col": 0.999999999}\n\
               {"string_col": "2nd value", "numeric_col": 0.000000002}"""
    jsonfile = io.StringIO(data)
    table = create_numeric_table
    job = bqclient.load_table_from_file(
        jsonfile, table, job_config=load_config
    ).result()
    if job.error_result:
        print("error")


def download_file(url):
    with urllib.request.urlopen(url) as response:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            shutil.copyfileobj(response, tmp_file)
    return tmp_file
