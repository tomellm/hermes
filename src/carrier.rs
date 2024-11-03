use diesel::{
    backend::Backend,
    r2d2::{ManageConnection, Pool},
    Connection,
};
use execute::ExecuteCarrier;
use query::QueryCarrier;
use tokio::sync::mpsc;

use crate::messenger::ContainerData;

pub mod execute;
pub mod query;

pub(crate) fn both_carriers<Database, DbValue>(
    pool: Pool<Database>,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
) -> (QueryCarrier<Database, DbValue>, ExecuteCarrier<Database>)
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
    DbValue: Send + 'static,
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
