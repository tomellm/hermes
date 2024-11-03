use std::sync::Arc;

use diesel::{
    backend::Backend,
    r2d2::{ManageConnection, Pool},
    Connection,
};
use projecting::ProjectingContainer;
use simple::Container;
use tokio::sync::mpsc;

use crate::{
    carrier::{self, execute::ExecuteCarrier, query::QueryCarrier},
    messenger::ContainerData,
};

pub mod projecting;
pub mod simple;

pub struct ContainerBuilder<Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    pool: Pool<Database>,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Database> ContainerBuilder<Database>
where
    Database: ManageConnection + 'static,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    pub fn new(
        pool: Pool<Database>,
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

    pub fn simple<DbValue>(self) -> Container<DbValue, Database>
    where
        DbValue: Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        Container::from_carriers(query, execute)
    }

    pub fn projector<Value, DbValue>(
        self,
        from_db_fn: impl Fn(DbValue) -> Value + Sync + Send + 'static,
    ) -> ProjectingContainer<Value, DbValue, Database>
    where
        Value: Send + 'static,
        DbValue: Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        ProjectingContainer::from_carriers(Arc::new(from_db_fn), query, execute)
    }

    pub fn projector_arc<Value, DbValue>(
        self,
        from_db_fn: Arc<dyn Fn(DbValue) -> Value + Sync + Send + 'static>,
    ) -> ProjectingContainer<Value, DbValue, Database>
    where
        Value: Send + 'static,
        DbValue: Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        ProjectingContainer::from_carriers(from_db_fn, query, execute)
    }

    fn new_carriers<DbValue>(&self) -> (QueryCarrier<Database, DbValue>, ExecuteCarrier<Database>)
    where
        DbValue: Send + 'static,
    {
        carrier::both_carriers(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
