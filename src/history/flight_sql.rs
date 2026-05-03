//! Arrow **Flight SQL** gRPC service backed by the same DataFusion session as the HTTP `/sql/...`
//! endpoint (`history` + `mqtt_hive`). ADBC and other Flight SQL clients connect here.

use std::sync::Arc;

use datafusion::execution::context::SQLOptions;
use datafusion_flight_sql_server::service::FlightSqlService;

use super::create_history_sql_session;
use super::History;
use super::HistoryAnalyticProfile;

/// Start a **Flight SQL** listener using [`super::create_history_sql_session`] so parquet /
/// LevelDB definitions match HTTP SQL.
pub async fn serve_history_flight_sql(
    history: Arc<History>,
    bind: String,
    profile: HistoryAnalyticProfile,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_history_sql_session(&history, profile).await?;
    let state = ctx.state().clone();

    let sql_options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    log::info!("Arrow Flight SQL listening on {bind} (ADBC / Flight SQL clients)");
    FlightSqlService::new(state)
        .with_sql_options(sql_options)
        .serve(bind)
        .await
}
