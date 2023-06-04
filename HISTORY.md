

# History and records


# querying the records

outputs are made using the parquet format. 
one can use datafusion-cli tools to sql query the event database

	datafusion-cli

	> select * from 'test.parquet' limit 10;

	> select distinct arrow_cast(topic, 'Utf8') as top, count(topic) from t group by top;


create a view with decoded elements :

create view history as select timestamp, arrow_cast(topic,'Utf8') as topic, arrow_cast(payload,'Utf8') as payload from 'h.parquet';


