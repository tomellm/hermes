use execute::ExecuteCarrier;
use manual_query::ManualQueryCarrier;
use sea_orm::{DatabaseConnection, EntityTrait};
use simple_query::SimpleQueryCarrier;
use tokio::sync::mpsc;

use crate::messenger::ContainerData;

pub mod execute;
pub mod manual_query;
pub mod query;
pub mod simple_query;

pub(crate) fn both_simple_carriers<DbValue>(
    pool: DatabaseConnection,
    name: String,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
) -> (SimpleQueryCarrier<DbValue>, ExecuteCarrier)
where
    DbValue: EntityTrait + Send + 'static,
{
    let query = SimpleQueryCarrier::register_new(
        name.clone(),
        pool.clone(),
        all_tables.clone(),
        tables_changed_sender.clone(),
        new_register_sender.clone(),
    );
    let execute = ExecuteCarrier::register_new(
        name,
        pool,
        all_tables,
        tables_changed_sender,
        new_register_sender,
    );
    (query, execute)
}

pub(crate) fn both_manual_carriers<DbValue>(
    pool: DatabaseConnection,
    name: String,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
) -> (ManualQueryCarrier<DbValue>, ExecuteCarrier)
where
    DbValue: Send + 'static,
{
    let query = ManualQueryCarrier::register_new(
        name.clone(),
        pool.clone(),
        all_tables.clone(),
        tables_changed_sender.clone(),
        new_register_sender.clone(),
    );
    let execute = ExecuteCarrier::register_new(
        name,
        pool,
        all_tables,
        tables_changed_sender,
        new_register_sender,
    );
    (query, execute)
}
