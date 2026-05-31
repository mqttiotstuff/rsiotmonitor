//! tests datafusion integration, to evaluate
//! the benefit of having a bundled history query system
//! in the software

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use arrow::array::ArrayRef;
use datafusion::arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_expr::expr::{BinaryExpr, Cast};
use datafusion_expr::{Expr, LogicalPlanBuilder, Operator, TableProviderFilterPushDown};
use datafusion_physical_expr::EquivalenceProperties;
use futures_core::Stream;
use leveldb::db::Database;
use leveldb::iterator::{Iterable, LevelDBIterator};
use leveldb::options::ReadOptions as LevelDbReadOptions;
use tokio::time::timeout;

use super::{History, TopicPayload};
use async_trait::async_trait;

const MAX_PACKET_SIZE: usize = 100_000;

/// Filters extracted from DataFusion `TableScan` predicates and applied while reading LevelDB.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistoryScanPushdown {
    pub topic_eq: Option<Vec<u8>>,
    pub timestamp_ge: Option<i64>,
    pub timestamp_gt: Option<i64>,
    pub timestamp_le: Option<i64>,
    pub timestamp_lt: Option<i64>,
}

impl HistoryScanPushdown {
    fn seek_timestamp(&self) -> Option<i64> {
        match (self.timestamp_ge, self.timestamp_gt) {
            (Some(ge), Some(gt)) => Some(ge.max(gt.saturating_add(1))),
            (Some(ge), None) => Some(ge),
            (None, Some(gt)) => Some(gt.saturating_add(1)),
            (None, None) => None,
        }
    }

    fn stop_after_timestamp(&self) -> Option<i64> {
        match (self.timestamp_le, self.timestamp_lt) {
            (Some(le), Some(lt)) => Some(le.min(lt.saturating_sub(1))),
            (Some(le), None) => Some(le),
            (None, Some(lt)) => Some(lt.saturating_sub(1)),
            (None, None) => None,
        }
    }

    fn timestamp_matches(&self, ts: i64) -> bool {
        if let Some(gt) = self.timestamp_gt {
            if ts <= gt {
                return false;
            }
        }
        if let Some(ge) = self.timestamp_ge {
            if ts < ge {
                return false;
            }
        }
        if let Some(lt) = self.timestamp_lt {
            if ts >= lt {
                return false;
            }
        }
        if let Some(le) = self.timestamp_le {
            if ts > le {
                return false;
            }
        }
        true
    }
}

fn parse_history_filters(filters: &[Expr]) -> HistoryScanPushdown {
    let mut pushdown = HistoryScanPushdown::default();
    for filter in filters {
        merge_filter_expr(filter, &mut pushdown);
    }
    pushdown
}

fn merge_filter_expr(expr: &Expr, pushdown: &mut HistoryScanPushdown) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            merge_filter_expr(left, pushdown);
            merge_filter_expr(right, pushdown);
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Some(col) = column_name(left) {
                apply_column_predicate(col, *op, right, pushdown);
            } else if let Some(col) = column_name(right) {
                apply_column_predicate_reversed(col, *op, left, pushdown);
            }
        }
        _ => {}
    }
}

fn apply_column_predicate(col: &str, op: Operator, other: &Expr, pushdown: &mut HistoryScanPushdown) {
    match col {
        "topic" if op == Operator::Eq => {
            if let Some(topic) = string_literal(other) {
                pushdown.topic_eq = Some(topic.into_bytes());
            }
        }
        "timestamp" => {
            if let Some(v) = int_literal(other) {
                apply_timestamp_op(op, v, pushdown);
            }
        }
        _ => {}
    }
}

fn apply_column_predicate_reversed(
    col: &str,
    op: Operator,
    other: &Expr,
    pushdown: &mut HistoryScanPushdown,
) {
    if col != "timestamp" {
        return;
    }
    let Some(v) = int_literal(other) else {
        return;
    };
    let reversed = match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Eq => Operator::Eq,
        _ => return,
    };
    apply_timestamp_op(reversed, v, pushdown);
}

fn apply_timestamp_op(op: Operator, v: i64, pushdown: &mut HistoryScanPushdown) {
    match op {
        Operator::Eq => {
            pushdown.timestamp_ge = Some(v);
            pushdown.timestamp_le = Some(v);
        }
        Operator::Gt => pushdown.timestamp_gt = Some(v),
        Operator::GtEq => pushdown.timestamp_ge = Some(v),
        Operator::Lt => pushdown.timestamp_lt = Some(v),
        Operator::LtEq => pushdown.timestamp_le = Some(v),
        _ => {}
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        _ => None,
    }
}

fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(s), _) => s.clone(),
        Expr::Literal(ScalarValue::LargeUtf8(s), _) => s.clone(),
        Expr::Cast(Cast { expr, .. }) => string_literal(expr),
        _ => None,
    }
}

fn int_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(v, _) => scalar_to_i64(v),
        Expr::Cast(Cast { expr, .. }) => int_literal(expr),
        _ => None,
    }
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

fn filter_is_pushdownable(expr: &Expr) -> bool {
    let mut parsed = HistoryScanPushdown::default();
    merge_filter_expr(expr, &mut parsed);
    parsed != HistoryScanPushdown::default()
}

#[derive(Clone, Copy, Debug, Default)]
struct ColumnNeeds {
    topic: bool,
    timestamp: bool,
    year: bool,
    month: bool,
    day: bool,
    payload: bool,
}

impl ColumnNeeds {
    fn from_schema(schema: &Schema) -> Self {
        let names: HashSet<&str> = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        Self {
            topic: names.contains("topic"),
            timestamp: names.contains("timestamp"),
            year: names.contains("year"),
            month: names.contains("month"),
            day: names.contains("day"),
            payload: names.contains("payload"),
        }
    }

    fn needs_date_parts(self) -> bool {
        self.year || self.month || self.day
    }
}

/// LevelDB iterator kept alive alongside its [`Database`] handle.
/// Used from a single async task; LevelDB's C iterator is not `Send` on its own.
struct LevelDbIter {
    _db: Arc<Database>,
    inner: leveldb::iterator::Iterator<'static>,
}

// SAFETY: LevelDBStream is consumed by one DataFusion partition task at a time.
unsafe impl Send for LevelDbIter {}

impl LevelDbIter {
    fn open(db: Arc<Database>, seek_key: Option<&[u8]>) -> Self {
        let mut inner = db.iter(&LevelDbReadOptions::new());
        if let Some(key) = seek_key {
            inner.seek(key);
        }
        // Iterator only holds a raw C pointer; Database lifetime is enforced by _db.
        let inner = unsafe { std::mem::transmute(inner) };
        Self { _db: db, inner }
    }

    fn next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.inner.valid() {
            return None;
        }
        let entry = self.inner.entry();
        self.inner.advance();
        Some(entry)
    }
}

/// Tuning for the DataFusion engine shared by HTTP `/sql` and Arrow Flight SQL.
/// Use `session_config_and_runtime` so both endpoints apply the same resource limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryAnalyticProfile {
    Small,
    Large,
    Default,
}

impl HistoryAnalyticProfile {
    /// [`SessionConfig`] and [`RuntimeEnv`] for this profile (same rules as the HTTP SQL handler).
    pub fn session_config_and_runtime(self) -> (SessionConfig, RuntimeEnv) {
        let runtime_env = match self {
            Self::Small => {
                let pool_size = 50 * 1024 * 1024;
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
                    .build()
                    .expect("RuntimeEnv (small analytic profile)")
            }
            Self::Large => {
                let pool_size = 1024 * 1024 * 1024;
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
                    .build()
                    .expect("RuntimeEnv (large analytic profile)")
            }
            Self::Default => RuntimeEnv::default(),
        };
        let config_env = match self {
            Self::Small => SessionConfig::new()
                .with_batch_size(512)
                .with_target_partitions(2),
            Self::Large => SessionConfig::new()
                .with_target_partitions(8)
                .with_batch_size(2048),
            Self::Default => SessionConfig::new(),
        };
        (config_env, runtime_env)
    }
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct CustomDataSource {
    inner: Arc<History>,
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("custom_db")
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(
            projections,
            filters,
            schema,
            self.clone(),
        )))
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    pushdown: HistoryScanPushdown,
    cache: Arc<PlanProperties>,
}

impl CustomExec {
    fn new(
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        schema: SchemaRef,
        db: CustomDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            projection: projections.cloned(),
            pushdown: parse_history_filters(filters),
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

pub struct LevelDBStream {
    schema: SchemaRef,
    projected_schema: Arc<Schema>,
    projection: Option<Vec<usize>>,
    pushdown: HistoryScanPushdown,
    column_needs: ColumnNeeds,
    iter: Option<LevelDbIter>,
    is_eof: bool,
}

// SAFETY: one partition task owns the stream; see LevelDbIter.
unsafe impl Send for LevelDBStream {}

impl LevelDBStream {
    fn try_new(
        database: Arc<Database>,
        schema: SchemaRef,
        projected_schema: Arc<Schema>,
        projection: Option<Vec<usize>>,
        pushdown: HistoryScanPushdown,
    ) -> Result<LevelDBStream> {
        let column_needs = ColumnNeeds::from_schema(&projected_schema);
        let seek_key = pushdown
            .seek_timestamp()
            .map(|ts| ts.to_be_bytes())
            .map(|bytes| bytes.to_vec());
        Ok(Self {
            schema,
            projected_schema,
            projection,
            pushdown,
            column_needs,
            iter: Some(LevelDbIter::open(database, seek_key.as_deref())),
            is_eof: false,
        })
    }

    fn row_matches(pushdown: &HistoryScanPushdown, timestamp: i64, value: &[u8]) -> bool {
        if !pushdown.timestamp_matches(timestamp) {
            return false;
        }
        if let Some(expected) = pushdown.topic_eq.as_deref() {
            if !TopicPayload::topic_bytes_match(value, expected) {
                return false;
            }
        }
        true
    }

    fn append_row(
        timestamp: i64,
        value: &[u8],
        needs: ColumnNeeds,
        all_topics: &mut Option<StringBuilder>,
        all_timestamps: &mut Option<Int64Builder>,
        all_year: &mut Option<Int32Builder>,
        all_month: &mut Option<Int32Builder>,
        all_days: &mut Option<Int32Builder>,
        all_payloads: &mut Option<BinaryBuilder>,
    ) {
        use chrono::Datelike;

        let (topic_bytes, payload) = if needs.topic || needs.payload {
            TopicPayload::split_value(value).unwrap_or((&[][..], &[][..]))
        } else {
            (&[][..], &[][..])
        };

        if needs.topic {
            let topic = std::str::from_utf8(topic_bytes).unwrap_or("");
            all_topics.as_mut().unwrap().append_value(topic);
        }
        if needs.timestamp {
            all_timestamps.as_mut().unwrap().append_value(timestamp);
        }
        if needs.payload {
            all_payloads.as_mut().unwrap().append_value(payload);
        }
        if needs.needs_date_parts() {
            let naive =
                chrono::NaiveDateTime::from_timestamp_opt(timestamp / 1_000_000, 0).unwrap();
            let date = naive.date();
            if needs.year {
                all_year.as_mut().unwrap().append_value(date.year());
            }
            if needs.month {
                all_month.as_mut().unwrap().append_value(date.month() as i32);
            }
            if needs.day {
                all_days.as_mut().unwrap().append_value(date.day() as i32);
            }
        }
    }
}

impl Stream for LevelDBStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.is_eof {
                return Poll::Ready(None);
            }

            let this = self.as_mut().get_mut();
            let iter = match this.iter.as_mut() {
                Some(it) => it,
                None => {
                    this.is_eof = true;
                    continue;
                }
            };

            let count_only = this.projected_schema.fields().is_empty();
            let needs = this.column_needs;
            let stop_after = this.pushdown.stop_after_timestamp();
            let pushdown = this.pushdown.clone();

            let mut all_topics = needs
                .topic
                .then(|| StringBuilder::with_capacity(MAX_PACKET_SIZE, MAX_PACKET_SIZE * 50));
            let mut all_timestamps = needs
                .timestamp
                .then(|| Int64Builder::with_capacity(MAX_PACKET_SIZE));
            let mut all_year = needs
                .year
                .then(|| Int32Builder::with_capacity(MAX_PACKET_SIZE));
            let mut all_month = needs
                .month
                .then(|| Int32Builder::with_capacity(MAX_PACKET_SIZE));
            let mut all_days = needs
                .day
                .then(|| Int32Builder::with_capacity(MAX_PACKET_SIZE));
            let mut all_payloads = needs
                .payload
                .then(|| BinaryBuilder::with_capacity(MAX_PACKET_SIZE, MAX_PACKET_SIZE * 1000));

            let mut rows_in_batch = 0usize;

            while rows_in_batch < MAX_PACKET_SIZE {
                let Some((key, value)) = iter.next_entry() else {
                    this.is_eof = true;
                    break;
                };

                use leveldb::database::util::FromU8;
                let timestamp = i64::from_u8(&key);

                if let Some(max_ts) = stop_after {
                    if timestamp > max_ts {
                        this.is_eof = true;
                        break;
                    }
                }

                if !Self::row_matches(&pushdown, timestamp, &value) {
                    continue;
                }

                rows_in_batch += 1;
                if !count_only {
                    Self::append_row(
                        timestamp,
                        &value,
                        needs,
                        &mut all_topics,
                        &mut all_timestamps,
                        &mut all_year,
                        &mut all_month,
                        &mut all_days,
                        &mut all_payloads,
                    );
                }
            }

            if rows_in_batch == 0 {
                continue;
            }

            let batch = if count_only {
                RecordBatch::try_new_with_options(
                    this.projected_schema.clone(),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(rows_in_batch)),
                )?
            } else {
                let mut result: Vec<ArrayRef> = Vec::new();
                for f in this.projected_schema.fields().iter() {
                    match f.name().as_str() {
                        "topic" => result.push(Arc::new(all_topics.take().unwrap().finish())),
                        "timestamp" => {
                            result.push(Arc::new(all_timestamps.take().unwrap().finish()))
                        }
                        "year" => result.push(Arc::new(all_year.take().unwrap().finish())),
                        "month" => result.push(Arc::new(all_month.take().unwrap().finish())),
                        "day" => result.push(Arc::new(all_days.take().unwrap().finish())),
                        "payload" => result.push(Arc::new(all_payloads.take().unwrap().finish())),
                        other => {
                            return Poll::Ready(Some(Err(
                                datafusion::error::DataFusionError::Internal(format!(
                                    "unknown history column {other}"
                                ))
                                .into(),
                            )));
                        }
                    }
                }
                RecordBatch::try_new(this.projected_schema.clone(), result)?
            };

            return Poll::Ready(Some(Ok(batch)));
        }
    }
}

impl RecordBatchStream for LevelDBStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &'static str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        let lstream = LevelDBStream::try_new(
            self.db.inner.database.clone(),
            self.schema(),
            self.projected_schema.clone(),
            self.projection.clone(),
            self.pushdown.clone(),
        )?;

        Ok(Box::pin(lstream))
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if filter_is_pushdownable(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection, filters, self.schema())
            .await
    }
}

/// Low-level: register `history` (LevelDB) and `mqtt_hive` (`history_archive` parquet). Prefer
/// [`create_history_sql_session`] for HTTP and Flight so profile + tables stay consistent.
pub async fn create_session(
    history_db: &Arc<History>,
    config: SessionConfig,
    runtime_env: RuntimeEnv,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    let db: CustomDataSource = CustomDataSource {
        inner: history_db.clone(),
    };

    ctx.register_table(TableReference::bare("history"), Arc::new(db))?;

    ctx.register_parquet(
        "history_archive",
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

    Ok(ctx)
}

/// [`SessionContext`] with `history` + `mqtt_hive` tables for the given analytic profile (shared by HTTP SQL and Flight SQL).
pub async fn create_history_sql_session(
    history_db: &Arc<History>,
    profile: HistoryAnalyticProfile,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let (config, runtime) = profile.session_config_and_runtime();
    create_session(history_db, config, runtime).await
}

#[cfg(test)]
mod pushdown_tests {
    use super::*;

    #[test]
    fn parse_topic_and_timestamp_filters() {
        let filters = vec![
            col("topic").eq(lit("home/esp13/sensors/presence")),
            col("timestamp").gt_eq(lit(1_000_i64)),
            col("timestamp").lt(lit(9_000_i64)),
        ];
        let pushdown = parse_history_filters(&filters);
        assert_eq!(
            pushdown.topic_eq.as_deref(),
            Some(b"home/esp13/sensors/presence".as_ref())
        );
        assert_eq!(pushdown.timestamp_ge, Some(1_000));
        assert_eq!(pushdown.timestamp_lt, Some(9_000));
        assert_eq!(pushdown.seek_timestamp(), Some(1_000));
        assert_eq!(pushdown.stop_after_timestamp(), Some(8_999));
    }

    #[test]
    fn parse_and_filter() {
        let filters = vec![col("topic")
            .eq(lit("a"))
            .and(col("timestamp").gt_eq(lit(5_i64)))];
        let pushdown = parse_history_filters(&filters);
        assert_eq!(pushdown.topic_eq.as_deref(), Some(b"a".as_ref()));
        assert_eq!(pushdown.timestamp_ge, Some(5));
    }
}

#[tokio::test]
async fn test_custom_history_dataframe() -> Result<()> {
    let init = History::init().unwrap();
    let ctx = SessionContext::new();
    let db: CustomDataSource = CustomDataSource { inner: init };

    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "history",
        provider_as_source(Arc::new(db)),
        None,
        vec![],
    )?
    .build()?;

    let mut dataframe = DataFrame::new(ctx.state(), logical_plan).select_columns(&[
        "topic",
        "timestamp",
        "year",
        "month",
        "day",
        "payload",
    ])?;

    timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();
        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

    Ok(())
}

#[tokio::test]
async fn test_sql_history_dataframe() -> Result<()> {
    let init = History::init().unwrap();
    let ctx = SessionContext::new();
    let db = CustomDataSource { inner: init };

    ctx.register_table(TableReference::bare("history"), Arc::new(db))?;

    let df = ctx
        .sql("SELECT * FROM history where topic='a'")
        .await?;
    df.show().await?;

    Ok(())
}

#[tokio::test]
async fn test_sql_create_external() -> Result<()> {
    let ctx = SessionContext::new();

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

    let df = ctx
        .sql("SELECT * FROM mqtt_hive limit 10")
        .await?;
    df.show().await?;

    Ok(())
}

#[tokio::test]
async fn test_count_star_from_history() -> Result<()> {
    let init = History::init().unwrap();
    let ctx = SessionContext::new();
    let db = CustomDataSource { inner: init };
    ctx.register_table(TableReference::bare("history"), Arc::new(db))?;

    let df = ctx.sql("SELECT count(*) FROM history").await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_avg_timestamp_from_history() -> Result<()> {
    let init = History::init().unwrap();
    let ctx = SessionContext::new();
    let db = CustomDataSource { inner: init };
    ctx.register_table(TableReference::bare("history"), Arc::new(db))?;

    let df = ctx.sql("SELECT avg(timestamp) FROM history").await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_avg_literal_from_history() -> Result<()> {
    let init = History::init().unwrap();
    let ctx = SessionContext::new();
    let db = CustomDataSource { inner: init };
    ctx.register_table(TableReference::bare("history"), Arc::new(db))?;

    let df = ctx.sql("SELECT avg(1) FROM history").await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    Ok(())
}
