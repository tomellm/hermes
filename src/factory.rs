use std::{fmt::Display, sync::{atomic::AtomicBool, Arc}};

use diesel::{backend::Backend, r2d2::{ManageConnection, Pool}, Connection};
use tokio::{sync::mpsc, task};

use crate::{container::Container, messenger::ContainerData};

pub struct Factory<Database>
where
    Database: ManageConnection,
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

    pub async fn register_new<Value>(&self) -> Container<Value, Database>
    where
        Value: Send + 'static,
    {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);
        let should_update = Arc::new(AtomicBool::new(false));

        let sender = self.new_register_sender.clone();
        let data = ContainerData::new(tables_interested_reciever, Arc::clone(&should_update));
        task::spawn(async move {
            let _ = sender.send(data).await;
        });

        Container::new(
            self.pool.clone(),
            self.all_tables.clone(),
            tables_interested_sender,
            self.tables_changed_sender.clone(),
            should_update,
            self.new_register_sender.clone(),
        )
    }
}
