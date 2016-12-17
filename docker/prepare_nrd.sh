#!/usr/bin/env bash
set -xe
docker cp config.json $(docker ps -l -q):/root/ComputationalHealthcare/
docker cp NRD_2013_Core.CSV $(docker ps -l -q):/root/data/CH/NRD/RAW/
docker exec -u="root" -it $(docker ps -l -q) fab prepare_nrd