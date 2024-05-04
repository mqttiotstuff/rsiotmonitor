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

use super::{History, TopicPayload};
use async_trait::async_trait;
use tokio::time::timeout;

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct CustomDataSource {
    inner: Arc<History>,
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("custom_db")
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl CustomExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: CustomDataSource) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &'static str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use leveldb::iterator::*;
        use leveldb::options::ReadOptions;
        let mut it = self.db.inner.database.iter(&ReadOptions::new());

        const MAX_PACKET_SIZE: usize = 10_000;

        let mut all_topics = StringBuilder::with_capacity(10_000, MAX_PACKET_SIZE);
        let mut all_timestamps = Int64Builder::with_capacity(MAX_PACKET_SIZE);
        let mut all_year = Int32Builder::with_capacity(MAX_PACKET_SIZE);
        let mut all_month = Int32Builder::with_capacity(MAX_PACKET_SIZE);
        let mut all_days = Int32Builder::with_capacity(MAX_PACKET_SIZE);
        let mut all_payloads = BinaryBuilder::with_capacity(10_000, MAX_PACKET_SIZE);

        let mut row = it.next();

        let mut last: Option<i64> = None;

        while row.is_some() {
            let mut cpt = 1;
            while row.is_some() && cpt % MAX_PACKET_SIZE != 0 {
                if let Some(a) = row.as_ref() {
                    use chrono::Datelike;
                    use leveldb::database::util::*;

                    let timestamp = i64::from_u8(&a.0);

                    let tp = TopicPayload::from_u8(&a.1);
                    let topic = tp.topic;
                    let payload = tp.payload;

                    all_topics.append_value(topic);
                    let b = payload.to_vec();
                    all_payloads.append_value(b);

                    let tbytes = timestamp;
                    all_timestamps.append_value(tbytes);

                    // year, month, day
                    let naive = chrono::NaiveDateTime::from_timestamp_opt(timestamp / 1_000_000, 0)
                        .unwrap();
                    let date = naive.date();
                    let year: i32 = date.year();
                    all_year.append_value(year);
                    let month: i32 = date.month().try_into().unwrap();
                    all_month.append_value(month);

                    let day: i32 = date.day().try_into().unwrap();
                    all_days.append_value(day);
                }

                cpt += 1;
                row = it.next();
            }
        }

        let mut result : Vec<ArrayRef> = Vec::new();
        for f in self.projected_schema.fields().iter() {
            if f.name() == "topic" {
                result.push(Arc::new(all_topics.finish()));
            } else if f.name() == "timestamp" {
                result.push(  Arc::new(all_timestamps.finish()));
            } else if f.name() == "year" {
                result.push(  Arc::new(all_year.finish()));
            }
            else if f.name() == "month" {
                result.push(Arc::new(all_month.finish()));
            }else if f.name() == "day" {
                result.push(Arc::new(all_days.finish()));
            }else if f.name() == "payload" {
                result.push(Arc::new(all_payloads.finish()));
            }
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                result
              /*  vec![
                    Arc::new(all_topics.finish()),
                    Arc::new(all_timestamps.finish()),
                    Arc::new(all_year.finish()),
                    Arc::new(all_month.finish()),
                    Arc::new(all_days.finish()),
                    Arc::new(all_payloads.finish()),
                ],
                 */
            )?],
            self.schema(),
            None,
        )?))
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // let mut all_topics = StringBuilder::with_capacity(10_000,MAX_PACKET_SIZE);
    // let mut all_timestamps = Int64Builder::with_capacity(MAX_PACKET_SIZE);
    // let mut all_year = Int32Builder::with_capacity(MAX_PACKET_SIZE);
    // let mut all_month = Int32Builder::with_capacity(MAX_PACKET_SIZE);
    // let mut all_days = Int32Builder::with_capacity(MAX_PACKET_SIZE);
    // let mut all_payloads = BinaryBuilder::with_capacity(10_000,MAX_PACKET_SIZE);

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("topic", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("year", DataType::Int32, false),
            Field::new("month", DataType::Int32, false),
            Field::new("day", DataType::Int32, false),
            Field::new("payload", DataType::Binary, false),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

/**
 * create a new DataFusion Session
 */
pub async fn create_session(
    history_db: &Arc<History>,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // create local execution context
    let ctx = SessionContext::new();

    let db: CustomDataSource = CustomDataSource {
        inner: history_db.clone(),
    };

    // register parquet file with the execution context
    ctx.register_table(TableReference::bare("history"), Arc::new(db))?;

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

    return Ok(ctx);
}

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
            "SELECT * \
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
            "SELECT * \
     FROM mqtt_hive limit 10",
        )
        .await?;
    // print the results
    df.show().await?;

    Ok(())
}
