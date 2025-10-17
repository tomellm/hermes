use std::collections::HashSet;

use chrono::{DateTime, FixedOffset, Local};
use sea_orm::{EntityTrait, QuerySelect, Select};

use crate::consts::QUERY_BUILDER;

pub mod actor;
pub mod carrier;
mod consts;
pub mod container;
pub mod factory;
pub mod messenger;

fn get_tables_present(all_tables: &[String], query: &str) -> Vec<String> {
    all_tables
        .iter()
        .filter_map(|table| query.find(table.as_str()).map(|_| table.clone()))
        .collect::<Vec<_>>()
}

pub trait ContainsTables {
    fn and_find_tables(self, collector: &mut TablesCollector) -> Self;
}

impl<T> ContainsTables for Select<T>
where
    T: EntityTrait,
{
    fn and_find_tables(mut self, collector: &mut TablesCollector) -> Self {
        collector.add(self.query().to_string(QUERY_BUILDER).as_str());
        self
    }
}

pub struct TablesCollector {
    time_started: DateTime<FixedOffset>,
    all_tables: Vec<String>,
    tables: HashSet<String>,
}

impl TablesCollector {
    pub fn new(all_tables: Vec<String>) -> Self {
        Self {
            time_started: Local::now().into(),
            all_tables,
            tables: HashSet::new(),
        }
    }

    pub fn add(&mut self, query: &str) {
        let found_tables = get_tables_present(&self.all_tables, query);
        self.tables.extend(found_tables);
    }
}

pub trait ToActiveModel {
    type ActiveModel;
    fn dml_clone(&self) -> Self::ActiveModel;
    fn dml(self) -> Self::ActiveModel;
}

#[macro_export]
macro_rules! impl_to_active_model {
    ($type:ty, $dbtype:ty) => {
        impl $crate::ToActiveModel for $type {
            type ActiveModel = ActiveModel;
            fn dml_clone(&self) -> Self::ActiveModel {
                ActiveModel::from(<$dbtype as FromEntity<$type>>::from_entity(self.clone()))
            }
            fn dml(self) -> Self::ActiveModel {
                ActiveModel::from(<$dbtype as FromEntity<$type>>::from_entity(self))
            }
        }
    };
}

pub trait ToEntity<Entity>
where
    Entity: ?Sized,
{
    fn to_entity(self) -> Entity;
}

pub trait FromEntity<Entity>
where
    Entity: ?Sized,
{
    fn from_entity(entity: Entity) -> Self;
}
