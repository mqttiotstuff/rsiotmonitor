
# WORK IN PROGRESS - REPOSITORY


This is new rust implementation of the iotmonitor project. 


## RoadMap

    [x] monitoring process
    [x] states management and restoration
    [x] mqtt integration (builtin or )

    [ ] web api for device and agents information

    [x] history save, and SQL querying.
    [x] rotating parquet file creation.
    
    [x] dynamic query the history, (either a flat parquet file providing, or datafusion arrow flight protocol). may be an inflight sql integration, With a custom source, see the best approach to integrate the query, using datafusion
    or flat parquet file providing, using external softwares.

    [] light agent rules definitions
