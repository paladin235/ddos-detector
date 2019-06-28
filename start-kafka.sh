#!/usr/bin/env bash
set -e
KAFKA_HOME="$HOME/kafka_2.12-2.3.0"
pushd $KAFKA_HOME
trap popd exit
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 3
bin/kafka-server-start.sh config/server.properties &
