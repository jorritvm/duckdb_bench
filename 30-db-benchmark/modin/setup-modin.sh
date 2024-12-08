#!/bin/bash
set -e

virtualenv modin/py-modin --python=python3
source modin/py-modin/bin/activate

# install binaries
python3 -m pip install --upgrade modin[all]

# check
python3
import modin
modin.__version__
quit()

deactivate
