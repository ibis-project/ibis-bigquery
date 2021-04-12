#!/bin/bash

set -e
set -x

python -m pip install --upgrade pip
python -m pip install ./ibis
python -m pip install .
python -m pip install pytest
