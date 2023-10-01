


# History and records


# querying the records

outputs are made using the parquet format. 
one can use datafusion-cli tools to sql query the event database

	datafusion-cli

	> select * from 'test.parquet' limit 10;

	> select distinct arrow_cast(topic, 'Utf8') as top, count(topic) from t group by top;


create a view with decoded elements :

create view history as select arrow_cast(arrow_cast(timestamp,'Int64')*1000,'Timestamp(Nanosecond,None)') as timestamp, arrow_cast(topic,'Utf8') as topic, arrow_cast(payload,'Utf8') as payload from 'history_archive';



select * from history where starts_with(topic,'home/agents/presence/patrice') order by timestamp desc;



select *,date_trunc('day', timestamp) as day from history where topic = 'home/agents/daylight/light' order by timestamp desc;



## Test insert into

create external table light (timestamp TIMESTAMP, r INT) STORED as CSV LOCATION 'light.csv';

insert into light select timestamp, arrow_cast(payload,'Int32') as r from history where topic = 'home/agents/daylight/light';

