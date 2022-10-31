#!/usr/bin/env bash

set -ex

python -m pip install --upgrade pip

if [ "$IBIS_VERSION" = "github" ] ; then
    # these deps are to ensure that unreleased versions of upstream ibis are
    # reflected in its version
    python -m pip install --upgrade 'poetry>=1.2' 'poetry-dynamic-versioning>=0.18.0'
    python -m pip install 'git+https://github.com/ibis-project/ibis.git@master'
    python -c 'import ibis; print(ibis.__version__)'
else
    python -m pip install ibis-framework=="$IBIS_VERSION"
fi

echo "$PWD"
python -m pip install ./ibis_bigquery
python -m pip install pytest
