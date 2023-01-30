
launch the stress test :


	#!/bin/bash

	for i in {1..10000}; 
	do
	mosquitto_pub -h localhost -p 1884 -m "hello${i}" -t stress_topic  -q 1 -V 5
	done


use@alexa:~/rsiotmonitor/validation_and_tests$ time ./stress.sh 

real	0m37,592s
user	0m14,561s
sys	0m12,065s

>>> 266 message/s en execution process (mosquitto_pub)

