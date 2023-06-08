


create view esp3_wifi as select timestamp, split_part(payload,',',1) as spot, arrow_cast(split_part(payload,',',2), 'Int64') as rssi from history where topic = 'home/esp03/sensors/wifilocation' and strpos(split_part(payload,',',2),':') = 0 ;


 select * from esp3_wifi limit 10;
+------------------+----------------------------+------+
| timestamp        | spot                       | rssi |
+------------------+----------------------------+------+
| 1685813556563456 | freebox_VPEXBN:4           | -91  |
| 1685813466519040 | Livebox-3FD0:3             | -73  |
| 1685813481490176 | iot_:2                     | -60  |
| 1685813349597697 | ESP_064FA5:0               | -74  |
| 1685813379694849 | SFR WiFi FON:0             | -94  |
| 1685813511883521 | HAZIZA:3                   | -83  |
| 1685813364714241 | Livebox-E4B8_wifi_invite:3 | -79  |
| 1685813466492930 | FreeWifi_secure:5          | -92  |
| 1685813184694275 | Livebox-E4B8_wifi_invite:3 | -74  |
| 1685813319665667 | Livebox-5730:3             | -83  |
+------------------+----------------------------+------+
10 rows in set. Query took 0.012 seconds.


analyse de moyennes glissantes :

select spot, rssi, avg(rssi) over (order by timestamp asc range between 3 PRECEDING and 3 FOLLOWING ) from esp3_wifi;

