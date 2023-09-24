


create external table mqtt(timestamp bigint, topic bytea, payload bytea) stored as parquet with order (timestamp)   location 'history_archive/*.parquet';

note : partition by cannot be done, in hive format (need stable sharding, timestamp is not enougth)

select * from mqtt where arrow_cast(timestamp,'Timestamp(Microsecond,None)') > to_timestamp('2023-09-07') limit 10;

// create an extract with month and year extracted
copy (select timestamp, topic, payload, arrow_cast(date_part('day',arrow_cast(timestamp,'Timestamp(Microsecond,None)')), 'Int8') as day, arrow_cast(date_part('month',arrow_cast(timestamp,'Timestamp(Microsecond,None)')), 'Int8') as month, arrow_cast(date_part('year',arrow_cast(timestamp,'Timestamp(Microsecond,None)')), 'Int16') as year from mqtt) to 'events_with_date' (FORMAT parquet,SINGLE_FILE_OUTPUT false);


### datashared:


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


