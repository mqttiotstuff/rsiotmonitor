//! tests datafusion integration, to evaluate
//! the benefit of having a bundled history query system
//! in the software

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::ArrayRef;
use datafusion::arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use datafusion_expr::{Expr, LogicalPlanBuilder};
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use rsiotmonitor::history::{History, TopicPayload};
use tokio::time::timeout;



#[tokio::test]
async fn test_custom_history_dataframe() -> Result<()> {
    // fields stored in the history leveldb database :
    //
    // Field::new("topic", DataType::Utf8, false),
    // Field::new("timestamp", DataType::Int64, false),
    // Field::new("year", DataType::Int32, false),
    // Field::new("month", DataType::Int32, false),
    // Field::new("day", DataType::Int32, false),
    // Field::new("payload", DataType::Binary, false),

    let init = History::init().unwrap();

    // create local execution context
    let ctx = SessionContext::new();

    let db: CustomDataSource = CustomDataSource { inner: init };

    // create logical plan composed of a single TableScan
    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "history",
        provider_as_source(Arc::new(db)),
        None,
        vec![],
    )?
    .build()?;

    let filter = None;

    let mut dataframe = DataFrame::new(ctx.state(), logical_plan).select_columns(&[
        "topic",
        "timestamp",
        "year",
        "month",
        "day",
        "payload",
    ])?;

    if let Some(f) = filter {
        dataframe = dataframe.filter(f)?;
    }

    timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();

        // assert_eq!(expected_result_length, record_batch.column(1).len());
        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

    Ok(())
}

#[tokio::test]
async fn test_sql_history_dataframe() -> Result<()> {
    let init = History::init().unwrap();

    // create local execution context
    let ctx = SessionContext::new();

    let db = CustomDataSource { inner: init };

    // register parquet file with the execution context
    let register_result = ctx
        .register_table(TableReference::bare("history"), Arc::new(db))
        .unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT topic \
        FROM history where topic='a'",
        )
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}

#[tokio::test]
async fn test_sql_create_external() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // put in place a similar declaration of : "create external table mqtt_hive(year int,month int, day int, timestamp bigint, topic bytea, payload bytea) stored as parquet partitioned by (year,month,day)  location 'history_archive';";
    // execute the query
    ctx.register_parquet(
        "mqtt_hive",
        "history_archive",
        ParquetReadOptions {
            table_partition_cols: vec![
                ("year".into(), DataType::Int32),
                ("month".into(), DataType::Int32),
                ("day".into(), DataType::Int32),
            ],
            schema: Some(&Schema::new(vec![
                Field::new("topic", DataType::Utf8, false),
                Field::new("timestamp", DataType::Int64, false),
                Field::new("payload", DataType::Binary, false),
            ])),

            ..Default::default()
        },
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT topic \
     FROM mqtt_hive limit 10",
        )
        .await?;
    // print the results
    df.show().await?;

    Ok(())
}
