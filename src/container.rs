use std::sync::Arc;

use diesel::{
    backend::Backend,
    r2d2::{ManageConnection, Pool},
    Connection,
};
use projecting::ProjectingContainer;
use simple::Container;
use tokio::sync::mpsc;

use crate::{carrier::Carrier, messenger::ContainerData};

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
        Container::from_carrier(self.new_carrier())
    }

    pub fn projector<Value, DbValue>(
        self,
        from_db_fn: impl Fn(DbValue) -> Value + Send + 'static,
    ) -> ProjectingContainer<Value, DbValue, Database>
    where
        Value: Send + 'static,
        DbValue: Send + 'static,
    {
        ProjectingContainer::from_carrier(Arc::new(from_db_fn), self.new_carrier())
    }

    pub fn projector_arc<Value, DbValue>(
        self,
        from_db_fn: Arc<dyn Fn(DbValue) -> Value + Send + 'static>,
    ) -> ProjectingContainer<Value, DbValue, Database>
    where
        Value: Send + 'static,
        DbValue: Send + 'static,
    {
        ProjectingContainer::from_carrier(from_db_fn, self.new_carrier())
    }

    fn new_carrier<DbValue>(&self) -> Carrier<Database, DbValue>
    where
        DbValue: Send + 'static,
    {
        Carrier::register_new(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
