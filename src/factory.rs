use std::sync::Arc;

use sqlx::{Database, Executor, Pool};
use tokio::sync::mpsc;

use crate::{container::ContainerBuilder, messenger::ContainerData};

pub struct Factory<DB>
where
    DB: Database,
{
    pool: Arc<Pool<DB>>,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<DB> Clone for Factory<DB>
where
    DB: Database,
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

impl<DB> Factory<DB>
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

    pub fn builder(&self) -> ContainerBuilder<DB> {
        ContainerBuilder::new(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
