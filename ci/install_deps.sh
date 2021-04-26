#!/bin/bash

set -e
set -x

python -m pip install --upgrade pip
python -m pip install --user -e ./ibis
python -m pip install --user -e .
python -m pip install --user pytest
