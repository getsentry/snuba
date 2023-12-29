#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..

OLD_VERSION="$1"
NEW_VERSION="$2"

sed -i -e "s/^VERSION = "'".*"'"\$/VERSION = "'"'"$NEW_VERSION"'"'"/" setup.py
sed -i -e "s/^release = "'".*"'"\$/release = "'"'"$NEW_VERSION"'"'"/" ./docs/source/conf.py

echo "New version: $NEW_VERSION"
