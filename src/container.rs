use projecting::ProjectingContainer;
use sea_orm::{DatabaseConnection, EntityTrait};
use simple::Container;
use sqlx_projector::projectors::{FromEntity, ToEntity};
use tokio::sync::mpsc;

use crate::{
    carrier::{self, execute::ExecuteCarrier, query::QueryCarrier},
    messenger::ContainerData,
};

pub mod data;
pub mod projecting;
pub mod simple;

pub struct ContainerBuilder {
    pool: DatabaseConnection,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
}

impl ContainerBuilder {
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

    pub fn simple<DbValue>(self) -> Container<DbValue>
    where
        DbValue: EntityTrait + Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        Container::from_carriers(query, execute)
    }

    pub fn projector<Value, DbValue>(self) -> ProjectingContainer<Value, DbValue>
    where
        Value: Clone + Send + 'static,
        DbValue: EntityTrait + Send + 'static,
        <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
    {
        let (query, execute) = self.new_carriers();
        ProjectingContainer::from_carriers(query, execute)
    }

    fn new_carriers<DbValue>(&self) -> (QueryCarrier<DbValue>, ExecuteCarrier)
    where
        DbValue: EntityTrait + Send + 'static,
    {
        carrier::both_carriers(
            self.pool.clone(),
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
