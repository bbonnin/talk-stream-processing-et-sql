#!/bin/bash

STORM_HOME=~/Perso/conferences/talk-stream-processing-et-sql/systems/apache-storm-1.2.2

$STORM_HOME/bin/storm nimbus &
sleep 10
$STORM_HOME/bin/storm supervisor &
sleep 10
$STORM_HOME/bin/storm ui &

