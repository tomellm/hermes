use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use diesel::Connection;
use tokio::sync::{mpsc, Mutex};

use crate::container::Container;

pub struct Messenger<Database>
where
    Database: Connection,
{
    pool: Arc<Mutex<Database>>,
    all_tables: Vec<String>,
    tables_changed: mpsc::Receiver<Vec<String>>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    container_data: Vec<ContainerData>,
    new_register_reciver: mpsc::Receiver<ContainerData>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl<Database> Messenger<Database>
where
    Database: Connection + 'static,
{
    pub fn new(pool: Arc<Mutex<Database>>) -> Self {
        let (tables_changed_sender, tables_changed) = mpsc::channel(50);
        let (new_register_sender, new_register_reciver) = mpsc::channel(5);
        Self {
            pool,
            all_tables: vec![],
            tables_changed,
            tables_changed_sender,
            container_data: vec![],
            new_register_reciver,
            new_register_sender
        }
    }

    pub fn new_container<Value>(&mut self) -> Container<Value, Database>
    where
        Value: Send + 'static,
    {
        let (tables_interested_sender, tables_interested_reciever) = mpsc::channel(3);
        let should_update = Arc::new(AtomicBool::new(false));

        self.container_data.push(ContainerData::new(
            tables_interested_reciever,
            Arc::clone(&should_update),
        ));

        Container::new(
            self.pool.clone(),
            self.all_tables.clone(),
            tables_interested_sender,
            self.tables_changed_sender.clone(),
            should_update,
            self.new_register_sender.clone()
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
            .filter(|container| container.interested(changed_tables.as_slice()))
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
    pub(crate) fn new(update_reciver: mpsc::Receiver<Vec<String>>, should_update: Arc<AtomicBool>) -> Self {
        Self {
            tables_interested: vec![],
            update_reciver,
            should_update,
        }
    }

    fn try_recv_and_update(&mut self) {
        if let Ok(tables) = self.update_reciver.try_recv() {
            self.tables_interested = tables;
        }
    }
    fn interested(&self, tables: &[String]) -> bool {
        tables
            .iter()
            .any(|table| self.tables_interested.contains(table))
    }
    fn should_update(&mut self) {
        self.should_update.store(true, Ordering::Relaxed);
    }
}
