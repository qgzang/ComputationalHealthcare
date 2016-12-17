#!/usr/bin/env bash
set -xe
docker exec -u="root" -it $(docker ps -l -q) bash