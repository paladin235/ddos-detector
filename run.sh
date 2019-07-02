#!/usr/bin/env bash
jarfile="ddos-detector-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f $jarfile ]; then
    mvn clean package
fi
KAFKA_HOME="$HOME/kafka_2.12-2.3.0"
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --delete --topic apache-log
java -jar $jarfile publish src/main/resources/log/apache-access-log.txt.gz
java -jar $jarfile detect results
