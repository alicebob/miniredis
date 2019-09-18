#!/bin/sh

set -eu

VERSION=5.0.5

rm -rf ./redis_src/
mkdir -p ./redis_src/
cd ./redis_src/
wget http://download.redis.io/releases/redis-${VERSION}.tar.gz -O ./redis.tar.gz
tar -xf ./redis.tar.gz
(cd ./redis-${VERSION}/src/ && make)
cp ./redis-${VERSION}/src/redis-server .
