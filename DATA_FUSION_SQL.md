


create external table mqtt(timestamp bigint, topic bytea, payload bytea) stored as parquet with order (timestamp)   location 'history_archive/*.parquet';

note : partition by cannot be done, in hive format (need stable sharding, timestamp is not enougth)

select * from mqtt where arrow_cast(timestamp,'Timestamp(Microsecond,None)') > to_timestamp('2023-09-07') limit 10;

// create an extract with month and year extracted
copy (select timestamp, topic, payload, arrow_cast(date_part('day',arrow_cast(timestamp,'Timestamp(Microsecond,None)')), 'Int8') as day, arrow_cast(date_part('month',arrow_cast(timestamp,'Timestamp(Microsecond,None)')), 'Int8') as month, arrow_cast(date_part('year',arrow_cast(timestamp,'Timestamp(Microsecond,None)')), 'Int16') as year from mqtt) to 'events_with_date' (FORMAT parquet,SINGLE_FILE_OUTPUT false);


### datashared using the hive partitioning:


create external table mqtt_hive(year int,month int, day int, timestamp bigint, topic bytea, payload bytea) stored as parquet partitioned by (year,month,day)  location 'history_archive';



ANALYZING ESP WIFI RECORDS:

create view esp3_wifi_location as select arrow_cast('esp03','Utf8') as device, arrow_cast(timestamp,'Timestamp(Microsecond,None)') as timestamp, split_part(arrow_cast(payload,'Utf8'),',',1) as spot, payload from mqtt where arrow_cast(topic,'Utf8') = 'home/esp03/sensors/wifilocation' ;

create view esp11_wifi_location as select arrow_cast('esp11','Utf8') as device, arrow_cast(timestamp,'Timestamp(Microsecond,None)') as timestamp, split_part(arrow_cast(payload,'Utf8'),',',1) as spot, payload from mqtt where arrow_cast(topic,'Utf8') = 'home/esp11/sensors/wifilocation' ;

### create a physical view of the wifi locations, for a given date :

copy (with x as (select * from esp3_wifi_location where timestamp > to_timestamp('2023-09-07') union select * from esp11_wifi_location where timestamp > to_timestamp('2023-09-07') ) select * from x order by timestamp) to 'wifi_location_device.parquet';

decode extra elements :

create or replace view wifi_locations as select timestamp, device, spot, arrow_cast(split_part(arrow_cast(payload,'Utf8'),',',2), 'Int64') as rssi from 'wifi_location_device.parquet' where strpos(split_part(arrow_cast(payload,'Utf8'),',',2),':') = 0 ;

select device, count(*) from wifi_locations group by device;

get current connected elements :

create or replace view evts as select date_trunc('day',arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as d,device,spot, count(timestamp) as nb, mean(rssi), stddev(rssi) from wifi_locations group by d,device,spot order by spot,d;

select * from evts;

un spot spécifique :

select * from wifi_locations where spot='ambrounette:3' order by timestamp;

select min(date_bin('1 day',timestamp, now())) from wifi_locations;

Weather:
--------

create view weather as select arrow_cast(timestamp,'Timestamp(Microsecond,None)') as timestamp, arrow_cast(payload,'Utf8') as payload from mqtt where arrow_cast(topic,'Utf8') = 'home/agents/weather/temperature' ;

select * from weather where timestamp > to_timestamp('2023-09-08');


Experiments :
--------

copy (with x as (select * from esp3_wifi_location union select * from esp11_wifi_location) select * from x) to 'wifi_location_device.parquet' (ROW_GROUP_LIMIT_BYTES 10000000);

create or replace view esp3_wifi as select timestamp, spot, arrow_cast(split_part(arrow_cast(payload,'Utf8'),',',2), 'Int64') as rssi from esp3_wifi_location where strpos(split_part(arrow_cast(payload,'Utf8'),',',2),':') = 0 ;
or :

copy (select timestamp, spot, arrow_cast(split_part(arrow_cast(payload,'Utf8'),',',2), 'Int64') as rssi from 'esp3.parquet' where strpos(split_part(arrow_cast(payload,'Utf8'),',',2),':') = 0 ) to 'esp3_wifi.parquet' (ROW_GROUP_LIMIT_BYTES 10000000);


create or replace view esp3_wifi_canal as select timestamp, split_part(spot,':',1) as spot, split_part(spot,':',2) as canal, rssi from esp3_wifi;

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



arrondir au jour la date :

select date_bin('1 day',timestamp, now()) from esp3_wifi

messages spots par jours perçu par esp3:
----------------------------------------

select date_trunc('day',arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as d,spot, count(timestamp) from esp3_wifi_canal group by d,spot order by d;

ou avec la table stockée :

create or replace view evts as select date_trunc('day',arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as d,spot, count(timestamp) as nb from 'esp3_wifi.parquet' group by d,spot order by d;



ANALYZING METEO_TEMPERATURE
---------------------------

create or replace view ext_temperature as select timestamp, arrow_cast(timestamp,'Timestamp(Microsecond,None)') as time, arrow_cast(arrow_cast(payload,'Utf8'),'Float32')/100 as temperature from '.' where arrow_cast(topic,'Utf8') = 'home/agents/meteo/temperature';

select date_trunc('day',arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as d,avg(temperature) from ext_temperature group by d order by d;


PRESENCE_BUREAU
---------------

select count(*),min(date_part('hour',timestamp)) as hour,month,day,min(date_part('dow',timestamp)) as dow from presence_hive where payload = 1 group by month,day,date_part('hour',timestamp) order by hour;

{"COUNT(*)":3,"day":28,"dow":3.0,"hour":20.0,"month":6}
{"COUNT(*)":84,"day":4,"dow":2.0,"hour":20.0,"month":7}
{"COUNT(*)":83,"day":28,"dow":4.0,"hour":20.0,"month":9}
{"COUNT(*)":1,"day":23,"dow":5.0,"hour":20.0,"month":6}
{"COUNT(*)":23,"day":25,"dow":2.0,"hour":20.0,"month":7}
{"COUNT(*)":2,"day":8,"dow":6.0,"hour":20.0,"month":7}
{"COUNT(*)":26,"day":18,"dow":2.0,"hour":20.0,"month":7}
{"COUNT(*)":5,"day":23,"dow":6.0,"hour":20.0,"month":9}
{"COUNT(*)":128,"day":17,"dow":6.0,"hour":20.0,"month":6}
{"COUNT(*)":67,"day":14,"dow":3.0,"hour":20.0,"month":6}
{"COUNT(*)":4,"day":7,"dow":5.0,"hour":20.0,"month":7}
{"COUNT(*)":82,"day":22,"dow":4.0,"hour":20.0,"month":6}
{"COUNT(*)":16,"day":2,"dow":0.0,"hour":20.0,"month":7}
{"COUNT(*)":170,"day":3,"dow":1.0,"hour":20.0,"month":7}
{"COUNT(*)":45,"day":26,"dow":6.0,"hour":20.0,"month":8}
{"COUNT(*)":2,"day":12,"dow":3.0,"hour":20.0,"month":7}
{"COUNT(*)":55,"day":21,"dow":5.0,"hour":20.0,"month":7}
{"COUNT(*)":6,"day":23,"dow":3.0,"hour":20.0,"month":8}
{"COUNT(*)":28,"day":20,"dow":4.0,"hour":20.0,"month":7}
{"COUNT(*)":111,"day":22,"dow":5.0,"hour":20.0,"month":9}
{"COUNT(*)":68,"day":18,"dow":0.0,"hour":20.0,"month":6}
{"COUNT(*)":42,"day":15,"dow":6.0,"hour":20.0,"month":7}
{"COUNT(*)":118,"day":26,"dow":3.0,"hour":20.0,"month":7}
{"COUNT(*)":105,"day":24,"dow":6.0,"hour":20.0,"month":6}
{"COUNT(*)":46,"day":5,"dow":3.0,"hour":20.0,"month":7}
{"COUNT(*)":19,"day":17,"dow":1.0,"hour":20.0,"month":7}
{"COUNT(*)":95,"day":25,"dow":0.0,"hour":20.0,"month":6}
{"COUNT(*)":22,"day":28,"dow":5.0,"hour":20.0,"month":7}
{"COUNT(*)":59,"day":16,"dow":0.0,"hour":20.0,"month":7}
{"COUNT(*)":118,"day":24,"dow":0.0,"hour":20.0,"month":9}
{"COUNT(*)":6,"day":22,"dow":2.0,"hour":20.0,"month":8}
{"COUNT(*)":28,"day":23,"dow":0.0,"hour":20.0,"month":7}
{"COUNT(*)":155,"day":8,"dow":4.0,"hour":20.0,"month":6}
{"COUNT(*)":2,"day":9,"dow":0.0,"hour":20.0,"month":7}
{"COUNT(*)":81,"day":12,"dow":1.0,"hour":20.0,"month":6}
{"COUNT(*)":66,"day":7,"dow":3.0,"hour":20.0,"month":6}
{"COUNT(*)":14,"day":25,"dow":5.0,"hour":20.0,"month":8}
{"COUNT(*)":70,"day":24,"dow":4.0,"hour":20.0,"month":8}
{"COUNT(*)":106,"day":21,"dow":3.0,"hour":20.0,"month":6}
{"COUNT(*)":1,"day":6,"dow":4.0,"hour":20.0,"month":7}
{"COUNT(*)":7,"day":10,"dow":6.0,"hour":20.0,"month":6}
{"COUNT(*)":78,"day":14,"dow":5.0,"hour":20.0,"month":7}
{"COUNT(*)":89,"day":11,"dow":0.0,"hour":20.0,"month":6}
{"COUNT(*)":2,"day":26,"dow":1.0,"hour":21.0,"month":6}
{"COUNT(*)":42,"day":28,"dow":4.0,"hour":21.0,"month":9}
{"COUNT(*)":8,"day":4,"dow":2.0,"hour":21.0,"month":7}
{"COUNT(*)":3,"day":8,"dow":6.0,"hour":21.0,"month":7}
{"COUNT(*)":28,"day":18,"dow":2.0,"hour":21.0,"month":7}
{"COUNT(*)":86,"day":17,"dow":6.0,"hour":21.0,"month":6}
{"COUNT(*)":16,"day":21,"dow":1.0,"hour":21.0,"month":8}
{"COUNT(*)":10,"day":20,"dow":0.0,"hour":21.0,"month":8}
{"COUNT(*)":4,"day":6,"dow":2.0,"hour":21.0,"month":6}
{"COUNT(*)":156,"day":3,"dow":1.0,"hour":21.0,"month":7}
{"COUNT(*)":3,"day":13,"dow":4.0,"hour":21.0,"month":7}
{"COUNT(*)":12,"day":22,"dow":4.0,"hour":21.0,"month":6}
{"COUNT(*)":130,"day":14,"dow":3.0,"hour":21.0,"month":6}
{"COUNT(*)":102,"day":9,"dow":5.0,"hour":21.0,"month":6}
{"COUNT(*)":14,"day":21,"dow":5.0,"hour":21.0,"month":7}
{"COUNT(*)":4,"day":22,"dow":2.0,"hour":21.0,"month":8}
{"COUNT(*)":71,"day":28,"dow":5.0,"hour":21.0,"month":7}
{"COUNT(*)":182,"day":8,"dow":4.0,"hour":21.0,"month":6}
{"COUNT(*)":27,"day":18,"dow":0.0,"hour":21.0,"month":6}
{"COUNT(*)":20,"day":23,"dow":0.0,"hour":21.0,"month":7}
{"COUNT(*)":29,"day":24,"dow":6.0,"hour":21.0,"month":6}
{"COUNT(*)":1,"day":26,"dow":3.0,"hour":21.0,"month":7}
{"COUNT(*)":108,"day":5,"dow":3.0,"hour":21.0,"month":7}
{"COUNT(*)":95,"day":24,"dow":0.0,"hour":21.0,"month":9}
{"COUNT(*)":65,"day":16,"dow":0.0,"hour":21.0,"month":7}
{"COUNT(*)":1,"day":17,"dow":1.0,"hour":21.0,"month":7}
{"COUNT(*)":58,"day":25,"dow":0.0,"hour":21.0,"month":6}
{"COUNT(*)":2,"day":9,"dow":0.0,"hour":21.0,"month":7}
{"COUNT(*)":121,"day":11,"dow":0.0,"hour":21.0,"month":6}
{"COUNT(*)":40,"day":13,"dow":2.0,"hour":21.0,"month":6}
{"COUNT(*)":38,"day":14,"dow":5.0,"hour":21.0,"month":7}
{"COUNT(*)":113,"day":12,"dow":1.0,"hour":21.0,"month":6}
{"COUNT(*)":53,"day":7,"dow":3.0,"hour":21.0,"month":6}
{"COUNT(*)":4,"day":20,"dow":2.0,"hour":21.0,"month":6}
{"COUNT(*)":29,"day":21,"dow":3.0,"hour":21.0,"month":6}
{"COUNT(*)":36,"day":19,"dow":3.0,"hour":21.0,"month":7}
{"COUNT(*)":50,"day":24,"dow":4.0,"hour":21.0,"month":8}
{"COUNT(*)":22,"day":18,"dow":2.0,"hour":22.0,"month":7}
{"COUNT(*)":8,"day":20,"dow":0.0,"hour":22.0,"month":8}
{"COUNT(*)":24,"day":29,"dow":2.0,"hour":22.0,"month":8}
{"COUNT(*)":62,"day":17,"dow":6.0,"hour":22.0,"month":6}
{"COUNT(*)":10,"day":28,"dow":4.0,"hour":22.0,"month":9}
{"COUNT(*)":3,"day":22,"dow":5.0,"hour":22.0,"month":9}
{"COUNT(*)":133,"day":3,"dow":1.0,"hour":22.0,"month":7}
{"COUNT(*)":4,"day":12,"dow":3.0,"hour":22.0,"month":7}
{"COUNT(*)":1,"day":28,"dow":1.0,"hour":22.0,"month":8}
{"COUNT(*)":70,"day":9,"dow":5.0,"hour":22.0,"month":6}
{"COUNT(*)":2,"day":23,"dow":3.0,"hour":22.0,"month":8}
{"COUNT(*)":4,"day":10,"dow":1.0,"hour":22.0,"month":7}
{"COUNT(*)":76,"day":14,"dow":3.0,"hour":22.0,"month":6}
{"COUNT(*)":1,"day":17,"dow":1.0,"hour":22.0,"month":7}
{"COUNT(*)":28,"day":25,"dow":0.0,"hour":22.0,"month":6}
{"COUNT(*)":17,"day":24,"dow":0.0,"hour":22.0,"month":9}
{"COUNT(*)":80,"day":5,"dow":3.0,"hour":22.0,"month":7}
{"COUNT(*)":34,"day":16,"dow":0.0,"hour":22.0,"month":7}
{"COUNT(*)":2,"day":24,"dow":6.0,"hour":22.0,"month":6}
{"COUNT(*)":22,"day":8,"dow":4.0,"hour":22.0,"month":6}
{"COUNT(*)":24,"day":22,"dow":2.0,"hour":22.0,"month":8}
{"COUNT(*)":5,"day":14,"dow":5.0,"hour":22.0,"month":7}
{"COUNT(*)":110,"day":12,"dow":1.0,"hour":22.0,"month":6}
{"COUNT(*)":13,"day":7,"dow":3.0,"hour":22.0,"month":6}
{"COUNT(*)":18,"day":20,"dow":0.0,"hour":23.0,"month":8}
{"COUNT(*)":3,"day":29,"dow":2.0,"hour":23.0,"month":8}
{"COUNT(*)":3,"day":23,"dow":6.0,"hour":23.0,"month":9}
{"COUNT(*)":2,"day":17,"dow":6.0,"hour":23.0,"month":6}
{"COUNT(*)":1,"day":28,"dow":3.0,"hour":23.0,"month":6}
{"COUNT(*)":11,"day":19,"dow":1.0,"hour":23.0,"month":6}
{"COUNT(*)":8,"day":3,"dow":1.0,"hour":23.0,"month":7}
{"COUNT(*)":2,"day":22,"dow":4.0,"hour":23.0,"month":6}
{"COUNT(*)":6,"day":22,"dow":2.0,"hour":23.0,"month":8}
{"COUNT(*)":2,"day":16,"dow":0.0,"hour":23.0,"month":7}
{"COUNT(*)":3,"day":17,"dow":1.0,"hour":23.0,"month":7}
{"COUNT(*)":5,"day":25,"dow":0.0,"hour":23.0,"month":6}
{"COUNT(*)":10,"day":12,"dow":1.0,"hour":23.0,"month":6}


