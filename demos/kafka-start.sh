#!/bin/bash

KAFKA_HOME=~/Perso/conferences/talk-stream-processing-et-sql/systems/kafka_2.11-1.1.0
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties 
