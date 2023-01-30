#!/bin/bash

for i in {1..10000}; 
do
mosquitto_pub -h localhost -p 1884 -m "hello${i}" -t stress_topic  -q 1 -V 5
done
