#!/usr/bin/env bash

set -e

echo "Building dask-cugraph"

conda build conda/recipes/dask-cugraph -c conda-forge -c numba -c rapidsai -c rapidsai-nightly -c nvidia -c pytorch --python=${PYTHON}
