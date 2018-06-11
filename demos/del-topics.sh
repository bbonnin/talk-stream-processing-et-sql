#!/bin/bash

KAFKA_HOME=~/Perso/conferences/talk-stream-processing-et-sql/systems/kafka_2.11-1.1.0

$KAFKA_HOME/bin/kafka-topics.sh --delete --topic weblogs --zookeeper localhost:2181
$KAFKA_HOME/bin/kafka-topics.sh --delete --topic errorlogs --zookeeper localhost:2181

