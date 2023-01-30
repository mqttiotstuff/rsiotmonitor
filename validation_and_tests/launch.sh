#!/bin/bash 

docker run --rm flaviostutz/mqtt-stresser -broker tcp://fhome.frett27.net:1884 -num-clients 10 -num-messages 10 -rampup-delay 1s -rampup-size 10 -global-timeout 180s -timeout 20s
