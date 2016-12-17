#!/usr/bin/env bash
set -xe
docker cp config.json $(docker ps -l -q):/root/ComputationalHealthcare/
docker cp PUDF_base1q2006_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base1q2007_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base1q2008_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base1q2009_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base2q2006_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base2q2007_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base2q2008_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base2q2009_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base3q2006_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base3q2007_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base3q2008_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base3q2009_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base4q2006_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base4q2007_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base4q2008_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker cp PUDF_base4q2009_tab.txt.gz $(docker ps -l -q):/root/data/CH/TX/RAW/
docker exec -u="root" -it $(docker ps -l -q) fab prepare_tx