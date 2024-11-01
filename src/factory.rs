use std::sync::{atomic::AtomicBool, Arc};

use diesel::{
    backend::Backend,
    r2d2::{ManageConnection, Pool},
    Connection,
};
use tokio::{sync::mpsc, task};

use crate::{
    carrier::Carrier,
    container::{projecting::ProjectingContainer, simple::Container, ContainerBuilder},
    messenger::ContainerData,
};

pub struct Factory<Database>
where
    Database: ManageConnection,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    pool: Pool<Database>,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Database> Factory<Database>
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

    pub fn builder<DbValue>(&self) -> ContainerBuilder<Database, DbValue>
    where
        DbValue: Send + 'static,
    {
        ContainerBuilder::new(self.)
    }

    pub fn register_simple<Value>(&self) -> Container<Value, Database>
    where
        Value: Send + 'static,
    {
        Container::from_carrier(self.new_carrier())
    }

    pub fn register_projector<Value, DbValue>(
        &self,
        from_db_fn: impl Fn(DbValue) -> Value + Send + 'static,
    ) -> ProjectingContainer<Value, DbValue, Database>
    where
        Value: Send + 'static,
        DbValue: Send + 'static,
    {
        ProjectingContainer::from_carrier(Arc::new(from_db_fn), self.new_carrier())
    }

    pub fn register_projector_arc<Value, DbValue>(
        &self,
        from_db: Arc<dyn Fn(DbValue) -> Value + Send + 'static>,
    ) -> ProjectingContainer<Value, DbValue, Database>
    where
        Value: Send + 'static,
        DbValue: Send + 'static,
    {
        ProjectingContainer::from_carrier(from_db, self.new_carrier())
    }
}
