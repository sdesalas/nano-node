#!/bin/bash
REPO_ROOT=`git rev-parse --show-toplevel`
pushd $REPO_ROOT
docker build -f docker/ci/Dockerfile-base -t nano-ci:base .
echo "Finished BASE, now building GCC"
sleep 5
docker build -f docker/ci/Dockerfile-gcc -t nano-ci:gcc .
popd
