#!/usr/bin/env bash
set -xe
docker build --build-arg CACHE_DATE=$(date +%Y-%m-%d:%H:%M:%S) -t computationalhealthcare .
