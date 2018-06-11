#!/bin/bash

KAFKA_HOME=~/Perso/conferences/talk-stream-processing-et-sql/systems/kafka_2.11-1.1.0

$KAFKA_HOME/bin/kafka-console-consumer.sh --topic errorlogs --bootstrap-server localhost:9092 --from-beginning

