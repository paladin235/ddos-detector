#!/usr/bin/env bash
jarfile="ddos-detector-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f $jarfile ]; then
    mvn clean package
fi
java -jar $jarfile src/main/resources/log/apache-access-log.txt.gz results/bot-ips.txt
