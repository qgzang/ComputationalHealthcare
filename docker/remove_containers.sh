#!/usr/bin/env bash
set -x
# This script removes all untagged containers, dangling nodes, and unused volumes.
# This might NOT be the behaviour you want!
docker rm $(docker ps -a -q)
docker images -q --filter "dangling=true" | xargs docker rmi
docker volume rm $(docker volume ls -qf dangling=true)