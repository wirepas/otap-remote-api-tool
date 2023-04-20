#!/bin/bash

# Clone the backend-apis repository and choose a July 3, 2020 commit
echo "#### Cloning the backend-apis repository"
echo
rm -rf backend-apis
git clone https://github.com/wirepas/backend-apis.git
cd backend-apis
git -c advice.detachedHead=false checkout a030461fcc618dd560d24aec3b3dc3e9440603d6
cd ..
echo

# Generate Python bindings for Protocol Buffers files
echo "#### Generating Python bindings (you can ignore warnings about unused imports)"
echo
mkdir -p proto-py
export SRC_DIR="backend-apis/gateway_to_backend/protocol_buffers_files"
for a in "$SRC_DIR"/*.proto; do
  protoc -I "$SRC_DIR" --python_out=proto-py "$a"
done
echo

# Clean up cloned repository
echo "#### Cleanup"
echo
rm -rf backend-apis
