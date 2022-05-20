import ibis  # noqa: F401
import pytest

import ibis_bigquery.client


@pytest.mark.parametrize(
    ["project", "dataset", "expected"],
    [
        ("my-project", "", ("my-project", "my-project", "")),
        (
            "my-project",
            "my_dataset",
            ("my-project", "my-project", "my_dataset"),
        ),
        (
            "billing-project",
            "data-project.my_dataset",
            ("data-project", "billing-project", "my_dataset"),
        ),
    ],
)
def test_parse_project_and_dataset(project, dataset, expected):
    got = ibis_bigquery.client.parse_project_and_dataset(project, dataset)
    assert got == expected


def test_parse_project_and_dataset_raises_error():
    expected_message = "data-project.my_dataset.table is not a BigQuery dataset"
    with pytest.raises(ValueError, match=expected_message):
        ibis_bigquery.client.parse_project_and_dataset(
            "my-project", "data-project.my_dataset.table"
        )
