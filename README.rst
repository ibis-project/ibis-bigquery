Ibis BigQuery backend
=====================

This package provides a [BigQuery](https://cloud.google.com/bigquery) backend
for [Ibis](https://ibis-project.org/).

Installation
------------

Supported Python Versions
^^^^^^^^^^^^^^^^^^^^^^^^^
Python >= 3.7, < 3.10

Unsupported Python Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Python < 3.7

Install with conda:

.. code-block:: console

    conda install -c conda-forge ibis-bigquery

Install with pip:

.. code-block:: console

    pip install ibis-bigquery

Usage
-----

Connecting to BigQuery
^^^^^^^^^^^^^^^^^^^^^^

Recommended usage (Ibis 2.x, only [not yet released]):

.. code-block:: python

    import ibis

    conn = ibis.bigquery.connect(
        project_id=YOUR_PROJECT_ID,
        dataset_id='bigquery-public-data.stackoverflow'
    )

Using this library directly:

.. code-block:: python

    import ibis
    import ibis_bigquery

    conn = ibis_bigquery.connect(
        project_id=YOUR_PROJECT_ID,
        dataset_id='bigquery-public-data.stackoverflow'
    )

Running a query
^^^^^^^^^^^^^^^

.. code-block:: python

    edu_table = conn.table(
        'international_education',
        database='bigquery-public-data.world_bank_intl_education')
    edu_table = edu_table['value', 'year', 'country_code', 'indicator_code']

    country_table = conn.table(
        'country_code_iso',
        database='bigquery-public-data.utility_us')
    country_table = country_table['country_name', 'alpha_3_code']

    expression = edu_table.join(
        country_table,
        [edu_table.country_code == country_table.alpha_3_code])

    print(conn.execute(
        expression[edu_table.year == 2016]
            # Adult literacy rate.
            [edu_table.indicator_code == 'SE.ADT.LITR.ZS']
            .sort_by([ibis.desc(edu_table.value)])
            .limit(20)
    ))
