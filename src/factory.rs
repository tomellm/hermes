use diesel::{
    backend::Backend,
    r2d2::{ManageConnection, Pool},
    Connection,
};
use tokio::sync::mpsc;

use crate::{container::ContainerBuilder, messenger::ContainerData};

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

impl<Database> Clone for Factory<Database>
where
    Database: ManageConnection,
    Database::Connection: Connection,
    <Database::Connection as Connection>::Backend: Default,
    <<Database::Connection as Connection>::Backend as Backend>::QueryBuilder: Default,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            all_tables: self.all_tables.clone(),
            tables_changed_sender: self.tables_changed_sender.clone(),
            new_register_sender: self.new_register_sender.clone(),
        }
    }
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

    pub fn builder(&self) -> ContainerBuilder<Database> {
        ContainerBuilder::new(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
