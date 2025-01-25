use execute::ExecuteCarrier;
use query::QueryCarrier;
use sea_orm::{DatabaseConnection, EntityTrait};
use tokio::sync::mpsc;

use crate::messenger::ContainerData;

pub mod execute;
pub mod query;

pub(crate) fn both_carriers<DbValue>(
    pool: DatabaseConnection,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
) -> (QueryCarrier<DbValue>, ExecuteCarrier)
where
    DbValue: EntityTrait + Send + 'static,
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
