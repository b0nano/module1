#!/bin/bash

sleep 10

topics=("cofee-order")

for topic in $topics
do
  /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0:9092 kafka-2:9092 kafka-person-3:9092 \
  --create --if-not-exists --topic $topic \
  --partitions 3 --replication-factor 2
done