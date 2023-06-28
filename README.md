
# WORK IN PROGRESS - REPOSITORY


This is new rust implementation of the iotmonitor project. 


## RoadMap

    [x] monitoring process
    [x] states management and restoring
    [x] mqtt integration
    

    [x] history saving.
    [x] rotating parquet file creation.
    
    [] dynamic query the history, (either a flat parquet file providing, or datafusion arrow flight protocol). may be an inflight sql integration, With a custom source, see the best approach to integrate the query, using datafusion
    or flat parquet file providing, using external softwares.


    [] web api for information
    [] light agent rules definitions
    [] application platform echosystem building
