#!/bin/bash

import os
import threading

def run():
    os.system("curl --http1.1 \"http://localhost:3000/sql/select%20date_trunc('hour'%2C%20arrow_cast(timestamp%2C'Timestamp(Microsecond%2CNone)'))%20as%20hour%2C%20sum(arrow_cast(arrow_cast(payload%2C'Utf8')%2C'Int32'))%20as%20avg_v%20from%20mqtt_hive%20where%20%20%20payload%3D'1'%20group%20by%20date_trunc('hour'%2C%20arrow_cast(timestamp%2C'Timestamp(Microsecond%2CNone)'))%20order%20by%20hour\"")

l = []
for i in range(0,10):
    t = threading.Thread(target=run)
    l.append(t)

for t in l:
    t.start()

for t in l:
    t.join()
    print(f"{t} joined")

