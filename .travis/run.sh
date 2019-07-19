#!/bin/bash

set -e
set -x

# Setup test data
TOPDIR=$PWD
pytest
