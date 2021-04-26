#!/bin/bash

set -e
set -x

# See https://github.com/pypa/pip/issues/7953
echo "import site
import sys
site.ENABLE_USER_SITE = '--user' in sys.argv[1:]
" >> ./ibis/setup.py

python -m pip install --upgrade pip
python -m pip install --user -e ./ibis
python -m pip install --user -e .
python -m pip install --user pytest
