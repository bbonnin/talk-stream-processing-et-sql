#!/bin/bash

STORM_HOME=~/Perso/conferences/talk-stream-processing-et-sql/systems/apache-storm-1.2.2

#$STORM_HOME/bin/storm sql storm-weblogs.sql --explain --artifacts "org.apache.storm:storm-sql-runtime:1.2.2,org.apache.storm:storm-sql-kafka:1.2.2,org.apache.storm:storm-kafka:1.2.2,org.apache.kafka:kafka_2.11:1.1.0^org.slf4j:slf4j-log4j12,org.apache.kafka:kafka-clients:1.1.0"

$STORM_HOME/bin/storm sql storm-weblogs.sql storm-error-logs --artifacts "org.apache.storm:storm-sql-runtime:1.2.2,org.apache.storm:storm-sql-kafka:1.2.2,org.apache.storm:storm-kafka:1.2.2,org.apache.kafka:kafka_2.10:0.8.2.2^org.slf4j:slf4j-log4j12,org.apache.kafka:kafka-clients:0.8.2.2"
