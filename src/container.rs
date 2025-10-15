use projecting::ProjectingContainer;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx_projector::projectors::{FromEntity, ToEntity};
use tokio::sync::mpsc;

use crate::{
    carrier::{self, execute::ExecuteCarrier, simple_query::SimpleQueryCarrier},
    messenger::ContainerData,
};

pub mod data;
pub mod manual;
pub mod projecting;
pub mod simple;

pub struct ContainerBuilder {
    pool: DatabaseConnection,
    all_tables: Vec<String>,
    tables_changed_sender: mpsc::Sender<Vec<String>>,
    new_register_sender: mpsc::Sender<ContainerData>,
    file: Option<String>,
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
            file: None,
        }
    }

    pub fn file(mut self, file: &str) -> Self {
        self.file = Some(file.into());
        self
    }

    fn final_name<ContType>(&self, cont_type: &str) -> String {
        format!(
            "{cont_type}_for_<{}>{}",
            std::any::type_name::<ContType>(),
            self.file
                .as_ref()
                .map(|f| format!("_in_<{f}>"))
                .unwrap_or_default()
        )
    }
    pub fn simple<DbValue>(self) -> simple::Container<DbValue>
    where
        DbValue: EntityTrait + Send + 'static,
    {
        let (query, execute) = self.new_carriers();
        simple::Container::from_carriers(query, execute)
    }

    pub fn projector<Value, DbValue>(self) -> ProjectingContainer<Value, DbValue>
    where
        Value: Clone + Send + 'static,
        DbValue: EntityTrait + Send + 'static,
        <DbValue as EntityTrait>::Model: FromEntity<Value> + ToEntity<Value>,
    {
        let (query, execute) = self.new_carriers();
        ProjectingContainer::from_carriers(self.final_name::<DbValue>("Projector"), query, execute)
    }

    pub fn manual<Value>(self) -> manual::Container<Value>
    where
        Value: Send + 'static,
    {
        let name = self.final_name::<Value>("Manual");
        let (query, execute) = carrier::both_manual_carriers(
            self.pool,
            name.clone(),
            self.all_tables,
            self.tables_changed_sender,
            self.new_register_sender,
        );
        manual::Container::from_carriers(name, query, execute)
    }

    fn new_carriers<DbValue>(&self) -> (SimpleQueryCarrier<DbValue>, ExecuteCarrier)
    where
        DbValue: EntityTrait + Send + 'static,
    {
        let name = self.final_name::<DbValue>("Simple");
        carrier::both_simple_carriers(
            self.pool.clone(),
            name,
            self.all_tables.clone(),
            self.tables_changed_sender.clone(),
            self.new_register_sender.clone(),
        )
    }
}
