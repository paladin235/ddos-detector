#!/bin/bash
set -e
KAFKA_HOME="$HOME/kafka_2.12-2.3.0"
$KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties &
$KAFKA_HOME/bin/kafka-server-start.sh config/server.properties &
