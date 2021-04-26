#!/bin/bash

set -e
set -x

python -m pip install --upgrade pip

if [ "$IBIS_VERSION" = "github" ] ; then
    # See https://github.com/pypa/pip/issues/7953
    echo "
import site
import sys
site.ENABLE_USER_SITE = '--user' in sys.argv[1:]
$(cat ./ibis/setup.py)" > ./ibis/setup.py

    python -m pip install --user -e ./ibis
else
    python -m pip install --user ibis-framework=="$IBIS_VERSION"
fi

python -m pip install --user -e .
python -m pip install --user pytest
