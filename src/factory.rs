use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;

use crate::{container::ContainerBuilder, messenger::ContainerData};

pub struct Factory {
    pool: DatabaseConnection,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl Clone for Factory {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            all_tables: self.all_tables.clone(),
            tables_changed_sender: self.tables_changed_sender.clone(),
            new_register_sender: self.new_register_sender.clone(),
        }
    }
}

impl Factory {
    pub fn new(
        pool: DatabaseConnection,
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

    pub fn builder(&self) -> ContainerBuilder {
        ContainerBuilder::new(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
