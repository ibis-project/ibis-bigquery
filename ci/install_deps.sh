#!/bin/bash

set -e
set -x

python -m pip install --upgrade pip
python -m pip install -e ./ibis
python -m pip install -e .
python -m pip install pytest
