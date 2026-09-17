#!/bin/bash

# Build script for grpc_bidir-app

set -e

# Check for required tools
command -v protoc >/dev/null 2>&1 || { echo "Error: protoc is not installed."; exit 1; }
command -v cmake >/dev/null 2>&1 || { echo "Error: cmake is not installed."; exit 1; }

echo "Cleaning up..."
rm -rf build

echo "Creating build directory..."
mkdir build
cd build

echo "Running cmake..."
# Using standard build path
cmake ..

echo "Running make..."
make -j 3

echo "Build complete."
