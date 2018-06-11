#!/bin/bash

KAFKA_HOME=~/Perso/conferences/talk-stream-processing-et-sql/systems/kafka_2.11-1.1.0
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic weblogs
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic errorlogs


# systems/kafka_2.11-1.1.0/bin/kafka-topics.sh --delete --topic weblogs --zookeeper localhost:2181
# systems/kafka_2.11-1.1.0/bin/kafka-topics.sh --list --zookeeper localhost:2181
