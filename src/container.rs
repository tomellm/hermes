use std::sync::Arc;

use projecting::ProjectingContainer;
use simple::Container;
use sqlx::{Database, Executor, FromRow, Pool};
use sqlx_projector::projectors::{FromEntity, ToEntity};
use tokio::sync::mpsc;

use crate::{
    carrier::{self, execute::ExecuteCarrier, query::QueryCarrier},
    messenger::ContainerData,
};

pub mod projecting;
pub mod simple;
pub mod data;

pub struct ContainerBuilder<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    pool: Arc<Pool<DB>>,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<DB> ContainerBuilder<DB>
where
    DB: Database,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
{
    pub fn new(
        pool: Arc<Pool<DB>>,
        all_tables: Vec<String>,
        tables_changed_sender: mpsc::Sender<Vec<String>>,
        new_register_sender: mpsc::Sender<ContainerData>,
    ) -> Self {
        Self {
            pool,
            all_tables,
            tables_changed_sender,
            new_register_sender,
        }
    }

    pub fn simple<DbValue>(self) -> Container<DbValue, DB>
    where
        for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        Container::from_carriers(query, execute)
    }

    pub fn projector<Value, DbValue>(self) -> ProjectingContainer<Value, DbValue, DB>
    where
        Value: Clone + Send + 'static,
        for<'row> DbValue:
            FromRow<'row, DB::Row> + FromEntity<Value> + ToEntity<Value> + Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        ProjectingContainer::from_carriers(query, execute)
    }

    fn new_carriers<DbValue>(&self) -> (QueryCarrier<DB, DbValue>, ExecuteCarrier<DB>)
    where
        for<'row> DbValue: FromRow<'row, DB::Row> + Send + 'static,
    {
        carrier::both_carriers(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
