#!/bin/bash

PKG_NAME=wirepas_mesh_messaging

# Find Python package directory
PKG_DIR=$(pip3 show "$PKG_NAME" 2>/dev/null | grep "^Location: " | cut -c11-)

# Verify that the package is installed
if test -z "$PKG_DIR"; then
  echo 1>&2 "package $PKG_NAME not installed"
  exit 1
fi

# Copy Python bindings for Protocol Buffers files
mkdir -p proto-py
SRC_DIR="$PKG_DIR/$PKG_NAME/proto/"
cp "$SRC_DIR"/*_pb2.py proto-py/
echo "copied from $SRC_DIR to proto-py/"
