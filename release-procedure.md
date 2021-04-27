
*   Review and merge PR from `release-please`. Check that it:
    * Updates version string in `ibis_bigquery/version.py`.
    * Includes all expected changes in `CHANGELOG.md`.

*   Checkout the code.

        git fetch upstream --tags
        git checkout vA.B.C

*   Build the package

        git clean -xfd
        python setup.py register sdist bdist_wheel


*   Upload to test PyPI

        twine upload --repository testpypi dist/*

*   Try out test PyPI package

        pip install --upgrade \
          --index-url https://test.pypi.org/simple/ \
          --extra-index-url https://pypi.org/simple \
          ibis-bigquery

*   Upload to PyPI

        twine upload dist/*


*   Find the [release on
    GitHub](https://github.com/ibis-project/ibis-bigquery/releases) using
    the tag created earlier.

    *   Verify the release notes.
    *   Upload wheel and source zip from `dist/` directory.

*   Do a pull-request to the feedstock on
    [ibis-bigquery-feedstock](https://github.com/conda-forge/ibis-bigquery-feedstock/)
    (Or review PR from @regro-cf-autotick-bot which updates the feedstock).

    *   update the version
    *   update the SHA256 (retrieve from PyPI)
    *   update the dependencies (if they changed)
