
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "description": "Capteur de présence",
   "data": {
    "url": "http://localhost:3000/sql/select arrow_cast(timestamp,'Timestamp(Microsecond,None)') as timestamp,arrow_cast(arrow_cast(payload,'Utf8'),'Int32') as v from mqtt_hive where topic='home%2Fesp13%2Fsensors%2Fpresence' limit 20000",
    "format": {"type": "csv" }
  },
  "mark": "point",
  "encoding": {
    "x": {"field": "timestamp", "timeUnit": "utcdate", "type": "temporal"},
    "y": {"field": "v", "type": "quantitative"}
  }
}


select arrow_cast(timestamp,'Timestamp(Microsecond,None)') as timestamp,arrow_cast(arrow_cast(payload,'Utf8'),'Int32') as v from mqtt_hive where topic='home/esp13/sensors/presence' limit 20000

-- Average of v values by hour
select date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as hour, avg(arrow_cast(arrow_cast(payload,'Utf8'),'Int32')) as avg_v from mqtt_hive where topic='home/esp13/sensors/presence' group by date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) order by hour


{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "description": "Présence en fonction de l'heure",
   "data": {
    "url": "http://localhost:3000/sql/select date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as hour, avg(arrow_cast(arrow_cast(payload,'Utf8'),'Int32')) as avg_v from mqtt_hive where topic='home%2Fesp13%2Fsensors%2Fpresence' group by date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) order by hour",
    "format": {"type": "csv" }
  },
  "mark": "circle",
  "encoding": {
    "x": {"field": "hour", "type": "temporal", "timeUnit":"utcdayhoursminutesseconds"},
    "y": {"field": "avg_v", "type": "quantitative"}
  }
}


// bar chart 
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
 "data": {
    "url": "http://localhost:3000/sql/select date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as hour, avg(arrow_cast(arrow_cast(payload,'Utf8'),'Int32')) as avg_v from mqtt_hive where topic='home%2Fesp13%2Fsensors%2Fpresence' group by date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) order by hour",
    "format": {"type": "csv" }
  },
  "mark": "bar",
  "encoding": {
    "x": {
     
      "field": "hour",
      "type":"ordinal"
    },
    "y": {
      "field": "avg_v",
      "type": "quantitative"
    }
  }
}


// punch card pour l'affichage de présence
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",

  "data": {
    "url": "http://fhome.frett27.net:3000/sql/select date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) as hour, sum(arrow_cast(arrow_cast(payload,'Utf8'),'Int32')) as avg_v from mqtt_hive where topic='home%2Fesp13%2Fsensors%2Fpresence' and year=2025 and month=12 and payload='1' group by date_trunc('hour', arrow_cast(timestamp,'Timestamp(Microsecond,None)')) order by hour ",
    "format": { "type": "csv" }
  },

  "transform": [
      {
    "calculate": "utcdate(datum.hour)",
    "as": "weekday_num"
  },
  {
    "calculate": "utchours(datum.hour)",
    "as": "hour_of_day_num"
  }
  ],
"width": 400,
"height": 500,
  "mark": "rect",

  "encoding": {
    "x": {
      "field": "weekday_num",
      "type": "ordinal",
      "title": "Jour du mois"
    },
    "y": {
      "field": "hour_of_day_num",
      "type": "ordinal",
      "title": "Heure du jour",
      "sort": "ascending"
    },
    "color": {
      "aggregate": "sum",
      "field": "avg_v",
      "type": "quantitative",
      "title": "Nombre evts"
    },
    "tooltip": [
      {
        "field": "weekday_num",
        "type": "ordinal",
        "title": "Jour",
        "format": ""
      },
      {
        "field": "hour_of_day_num",
        "type": "ordinal",
        "title": "Heure"
      },
      {
        "aggregate": "sum",
        "field": "avg_v",
        "type": "quantitative",
        "title": "Moyenne v"
      }
    ]
  }
}

