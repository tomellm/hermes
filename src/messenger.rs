use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{container::ContainerBuilder, factory::Factory};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use tokio::sync::mpsc;
use tracing::info;

pub struct Messenger {
    db: DatabaseConnection,
    all_tables: Vec<String>,
    tables_changed: mpsc::Receiver<Vec<String>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,

    container_data: Vec<ContainerData>,
    new_register_reciver: mpsc::Receiver<ContainerData>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl Messenger {
    pub async fn new(db: DatabaseConnection) -> Self {
        let (tables_changed_sender, tables_changed) = mpsc::channel(50);
        let (new_register_sender, new_register_reciver) = mpsc::channel(5);

        let all_tables = db
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                "select name from sqlite_master where type='table';",
            ))
            .await
            .unwrap()
            .into_iter()
            .map(|result| {
                let cell = result.try_get::<String>("", "name").unwrap();
                format!("\"{cell}\"")
            })
            .collect();

        Self {
            db,
            all_tables,
            tables_changed,
            tables_changed_sender,
            container_data: vec![],
            new_register_reciver,
            new_register_sender,
        }
    }

    pub fn builder(&self) -> ContainerBuilder {
        ContainerBuilder::new(
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }

    pub fn factory(&self) -> Factory {
        Factory::new(
            self.db.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }

    pub fn state_update(&mut self) {
        self.container_data
            .iter_mut()
            .for_each(ContainerData::try_recv_and_update);

        let mut changed_tables = HashSet::new();
        while let Ok(tables) = self.tables_changed.try_recv() {
            changed_tables.extend(tables);
        }
        let changed_tables = changed_tables.into_iter().collect::<Vec<_>>();
        self.container_data
            .iter_mut()
            .filter(|container| container.is_interested(changed_tables.as_slice()))
            .for_each(ContainerData::should_update);

        while let Ok(data) = self.new_register_reciver.try_recv() {
            self.container_data.push(data);
        }
    }
}

pub struct ContainerData {
    tables_interested: Vec<String>,
    update_reciver: mpsc::Receiver<Vec<String>>,
    should_update: Arc<AtomicBool>,
}

impl ContainerData {
    pub(crate) fn new(
        update_reciver: mpsc::Receiver<Vec<String>>,
        should_update: Arc<AtomicBool>,
    ) -> Self {
        Self {
            tables_interested: vec![],
            update_reciver,
            should_update,
        }
    }

    /// Try and recive new updates to the tables the Container is interested in
    fn try_recv_and_update(&mut self) {
        if let Ok(tables) = self.update_reciver.try_recv() {
            self.tables_interested = tables;
        }
    }

    /// Ask if this container is interested in the passed Tables
    fn is_interested(&self, tables: &[String]) -> bool {
        tables
            .iter()
            .any(|table| self.tables_interested.contains(table))
    }

    /// Tells the container to query again since the values might have changed
    fn should_update(&mut self) {
        self.should_update.store(true, Ordering::Relaxed);
    }
}
