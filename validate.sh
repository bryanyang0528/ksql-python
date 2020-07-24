#!/bin/sh
set -ex

black --line-length 120 --target-version py36 ksql tests
mypy ksql
flake8 setup.py ksql tests
