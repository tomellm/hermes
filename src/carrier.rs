use std::sync::Arc;

use execute::ExecuteCarrier;
use query::QueryCarrier;
use sqlx::{Database, Executor, FromRow, Pool};
use tokio::sync::mpsc;

use crate::messenger::ContainerData;

pub mod execute;
pub mod query;

pub(crate) fn both_carriers<DB, DbValue>(
    pool: Arc<Pool<DB>>,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
) -> (QueryCarrier<DB, DbValue>, ExecuteCarrier<DB>)
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
{
    let query = QueryCarrier::register_new(
        pool.clone(),
        all_tables.clone(),
        tables_changed_sender.clone(),
        new_register_sender.clone(),
    );
    let execute =
        ExecuteCarrier::register_new(pool, all_tables, tables_changed_sender, new_register_sender);
    (query, execute)
}


